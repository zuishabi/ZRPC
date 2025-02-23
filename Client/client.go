package Client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zuishabi/ZRPC/Server"
	"github.com/zuishabi/ZRPC/codec"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Call 承载一次RPC调用所需要的信息
type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{} //请求的信息
	Reply         interface{} //返回的信息
	Error         error
	Done          chan *Call
}

// 当调用结束后，会调用此函数通知调用方
func (call *Call) done() {
	call.Done <- call
}

// Client RPC客户端
type Client struct {
	cc       codec.Codec //消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应。
	opt      *Server.Option
	sending  sync.Mutex       //互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆
	header   codec.Header     //每个请求的消息头，header只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个，声明在Client结构体中可以复用
	mu       sync.Mutex       // protect following
	seq      uint64           //用于给发送的请求编号，每个请求拥有唯一编号。
	pending  map[uint64]*Call //存储未处理完的请求，键是编号，值是 Call 实例
	closing  bool             // 判断用户是否已经调用了CLose函数
	shutdown bool             // 一般有错误发生时为true
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable 当客户端可用时返回true
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// 将参数call从client pending中移除，同时返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	//将客户端的shutdown设置为false，同时关闭所有client.pending中的所有调用
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 客户端的接收功能，接收的相应有三种情况
// call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了。
// call 存在，但服务端处理出错，即 h.Error 不为空。
// call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值。
func (client *Client) receive() {
	var err error
	//先不断从连接中读取数据头
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			//代表当前写入失败，调用已经被删除
			err = client.cc.ReadBody(nil)

		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	//当出现错误，调用terminateCalls
	client.terminateCalls(err)
}

// NewClient 首先需要完成一开始的协议交换，即发送 Option 信息给服务端。协商好消息的编解码方式之后，再创建一个子协程调用 receive() 接收响应。
func NewClient(conn net.Conn, opt *Server.Option) (*Client, error) {
	f := codec.NewCodeFuncMap[opt.CodeType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodeType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	//将option发送给服务器
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Server.Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(opts ...*Server.Option) (*Server.Option, error) {
	//如果opts选项是空的或者内容是空的
	if len(opts) == 0 || opts[0] == nil {
		return Server.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = Server.DefaultOption.MagicNumber
	if opt.CodeType == "" {
		opt.CodeType = Server.DefaultOption.CodeType
	}
	return opt, nil
}

type clientResult struct {
	client *Client
	err    error
}
type newClientFunc func(conn net.Conn, opt *Server.Option) (client *Client, err error)

func dialTimeout(f newClientFunc, network, address string, opts ...*Server.Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectionTimeout)
	if err != nil {
		return nil, err
	}
	//如果客户端是空的，则关闭连接
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		//f为NewClient函数，创建一个客户端
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectionTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectionTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectionTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial 传递服务器的地址和选项，返回RPC客户端
func Dial(network, address string, opts ...*Server.Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

func (client *Client) send(call *Call) {
	//确保将数据全部发出
	client.sending.Lock()
	defer client.sending.Unlock()
	//将这个调用注册
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	//初始化请求头
	client.header.Seq = seq
	client.header.Error = ""
	client.header.ServiceMethod = call.ServiceMethod
	//将请求编码并发送
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 异步接口
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call 是对Go的封装，阻塞 call.Done，等待响应返回，是一个同步接口，超市处理机制使用context包，控制权交给用户，控制更加灵活
// 用户可以使用contect.WithTimeout创建具备超时检测能力的context对象来控制
// ctx, _ := context.WithTimeout(context.Background(), time.Second)
// var reply int
// err := client.Call(ctx, "Foo.Sum", &Args{1, 2}, &reply)
// ...
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}
