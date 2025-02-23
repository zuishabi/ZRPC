package Server

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zuishabi/ZRPC/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber       int
	CodeType          codec.Type
	ConnectionTimeout time.Duration
	HandleTimeout     time.Duration
}

var DefaultOption = &Option{
	MagicNumber:       MagicNumber,
	CodeType:          codec.GobType,
	ConnectionTimeout: 10 * time.Second,
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// Register 注册到默认服务器中
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

func (server *Server) findService(serviceMethod string) (svc *service, mType *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mType = svc.method[methodName]
	if mType == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// Accept 接受连接
func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server:accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// ServeConn 首先使用 json.NewDecoder 反序列化得到 Option 实例，检查 MagicNumber 和 CodeType 的值是否正确。
// 然后根据 CodeType 得到对应的消息编解码器，接下来的处理交给 serverCodec。
func (server *Server) ServeConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()
	var opt Option
	//只消耗需要的部分
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error:", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server:invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodeFuncMap[opt.CodeType]
	if f == nil {
		log.Printf("rpc server:invalid codec type %s", opt.CodeType)
		return
	}
	//将解码器传递进行操作，传递解码器
	server.serveCodec(f(conn), &opt)
}

var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	//用于确定一个response完整发出
	sending := new(sync.Mutex)
	//用于将所有的请求全部处理完成
	wg := new(sync.WaitGroup)
	for {
		//读取请求
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value // argv and replyv of request
	mtype        *methodType
	svc          *service
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	//创建请求的参数和返回值的实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()
	//确保argvi是一个指针，ReadBody需要一个指针作为参数
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}
