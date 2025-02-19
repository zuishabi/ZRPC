package codec

import "io"

type Header struct {
	ServiceMethod string //服务名和方法名
	Seq           uint64 //请求的序号，用来区分不同的请求
	Error         string //错误信息
}

// Codec 对消息体进行解码的接口，通过对应的生成函数，将服务器accept的连接进行打包
type Codec interface {
	io.Closer //用于在对象不再需要时释放资源或者执行清理函数
	ReadHeader(header *Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

//Codec的构造函数

type NewCodeFunc func(closer io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodeFuncMap map[Type]NewCodeFunc

func init() {
	NewCodeFuncMap = make(map[Type]NewCodeFunc)
	NewCodeFuncMap[GobType] = NewGobCodec
}
