package ipc

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"fmt"
	"github.com/hortonworks/gohadoop"
	"github.com/hortonworks/gohadoop/hadoop_common"
	"github.com/nu7hatch/gouuid"
	"log"
	"net"
	"strings"
	"sync"
)

type Client struct {
	ClientId      *uuid.UUID
	Ugi           *hadoop_common.UserInformationProto
	ServerAddress string
	TCPNoDelay    bool
}

type connection struct {
	con *net.TCPConn
}

type connection_id struct {
	user     string
	protocol string
}

type call struct {
	callId     int32
	procedure  proto.Message
	request    proto.Message
	response   proto.Message
	err        *error
	retryCount int32
}

func (c *Client) String() string {
	buf := bytes.NewBufferString("")
	fmt.Fprint(buf, "<clientId:", c.ClientId)
	fmt.Fprint(buf, ", server:", c.ServerAddress)
	fmt.Fprint(buf, ">")
	return buf.String()
}

func (c *Client) Call(rpc *hadoop_common.RequestHeaderProto, rpcRequest proto.Message, rpcResponse proto.Message) error {
	// Create connection_id
	connectionId := connection_id{user: *c.Ugi.RealUser, protocol: *rpc.DeclaringClassProtocolName}

	// Get connection to server
	//log.Println("Connecting...", c)
	conn, err := getConnection(c, &connectionId)
	if err != nil {
		return err
	}

	// Create call and send request
	rpcCall := call{callId: 0, procedure: rpc, request: rpcRequest, response: rpcResponse}
	err = sendRequest(c, conn, &rpcCall)
	if err != nil {
		log.Fatal("sendRequest", err)
		return err
	}

	// Read & return response
	err = c.readResponse(conn, &rpcCall)

	return err
}

var connectionPool = struct {
	sync.RWMutex
	connections map[connection_id]*connection
}{connections: make(map[connection_id]*connection)}

func getConnection(c *Client, connectionId *connection_id) (*connection, error) {
	// Try to re-use an existing connection
	connectionPool.RLock()
	con := connectionPool.connections[*connectionId]
	connectionPool.RUnlock()

	// If necessary, create a new connection and save it in the connection-pool
	var err error
	if con == nil {
		con, err = setupConnection(c)
		if err != nil {
			log.Fatal("Couldn't setup connection: ", err)
			return nil, err
		}

		connectionPool.Lock()
		connectionPool.connections[*connectionId] = con
		connectionPool.Unlock()

		writeConnectionHeader(con)
		writeConnectionContext(c, con, connectionId)
	}

	return con, nil
}

func setupConnection(c *Client) (*connection, error) {
	addr, _ := net.ResolveTCPAddr("tcp", c.ServerAddress)
	tcpConn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Println("error: ", err)
		return nil, err
	} else {
		log.Println("Successfully connected ", c)
	}

	// TODO: Ping thread

	// Set tcp no-delay
	tcpConn.SetNoDelay(c.TCPNoDelay)

	return &connection{tcpConn}, nil
}

func writeConnectionHeader(conn *connection) error {
	// RPC_HEADER
	if _, err := conn.con.Write(gohadoop.RPC_HEADER); err != nil {
		log.Fatal("conn.Write gohadoop.RPC_HEADER", err)
		return err
	}

	// RPC_VERSION
	if _, err := conn.con.Write(gohadoop.VERSION); err != nil {
		log.Fatal("conn.Write gohadoop.VERSION", err)
		return err
	}

	// RPC_SERVICE_CLASS
	if serviceClass, err := gohadoop.ConvertFixedToBytes(gohadoop.RPC_SERVICE_CLASS); err != nil {
		log.Fatal("binary.Write", err)
		return err
	} else if _, err := conn.con.Write(serviceClass); err != nil {
		log.Fatal("conn.Write RPC_SERVICE_CLASS", err)
		return err
	}

	// AuthProtocol
	if authProtocol, err := gohadoop.ConvertFixedToBytes(gohadoop.AUTH_PROTOCOL_NONE); err != nil {
		log.Fatal("WTF AUTH_PROTOCOL_NONE", err)
		return err
	} else if _, err := conn.con.Write(authProtocol); err != nil {
		log.Fatal("conn.Write gohadoop.AUTH_PROTOCOL_NONE", err)
		return err
	}

	return nil
}

func writeConnectionContext(c *Client, conn *connection, connectionId *connection_id) error {
	// Create hadoop_common.IpcConnectionContextProto
	ugi, _ := gohadoop.CreateSimpleUGIProto()
	ipcCtxProto := hadoop_common.IpcConnectionContextProto{UserInfo: ugi, Protocol: &connectionId.protocol}

	// Create RpcRequestHeaderProto
	var callId int32 = -3
	var clientId [16]byte = [16]byte(*c.ClientId)
	rpcReqHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &gohadoop.RPC_PROTOCOL_BUFFFER, RpcOp: &gohadoop.RPC_FINAL_PACKET, CallId: &callId, ClientId: clientId[0:16], RetryCount: &gohadoop.RPC_DEFAULT_RETRY_COUNT}

	rpcReqHeaderProtoBytes, err := proto.Marshal(&rpcReqHeaderProto)
	if err != nil {
		log.Fatal("proto.Marshal(&rpcReqHeaderProto)", err)
		return err
	}

	ipcCtxProtoBytes, _ := proto.Marshal(&ipcCtxProto)
	if err != nil {
		log.Fatal("proto.Marshal(&ipcCtxProto)", err)
		return err
	}

	totalLength := len(rpcReqHeaderProtoBytes) + sizeVarint(len(rpcReqHeaderProtoBytes)) + len(ipcCtxProtoBytes) + sizeVarint(len(ipcCtxProtoBytes))
	var tLen int32 = int32(totalLength)
	if totalLengthBytes, err := gohadoop.ConvertFixedToBytes(tLen); err != nil {
		log.Fatal("ConvertFixedToBytes(totalLength)", err)
		return err
	} else if _, err := conn.con.Write(totalLengthBytes); err != nil {
		log.Fatal("conn.con.Write(totalLengthBytes)", err)
		return err
	}

	if err := writeDelimitedBytes(conn, rpcReqHeaderProtoBytes); err != nil {
		log.Fatal("writeDelimitedBytes(conn, rpcReqHeaderProtoBytes)", err)
		return err
	}
	if err := writeDelimitedBytes(conn, ipcCtxProtoBytes); err != nil {
		log.Fatal("writeDelimitedBytes(conn, ipcCtxProtoBytes)", err)
		return err
	}

	return nil
}

func sizeVarint(x int) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func sendRequest(c *Client, conn *connection, rpcCall *call) error {
	//log.Println("About to call RPC: ", rpcCall.procedure)

	// 0. RpcRequestHeaderProto
	var clientId [16]byte = [16]byte(*c.ClientId)
	rpcReqHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &gohadoop.RPC_PROTOCOL_BUFFFER, RpcOp: &gohadoop.RPC_FINAL_PACKET, CallId: &rpcCall.callId, ClientId: clientId[0:16], RetryCount: &rpcCall.retryCount}
	rpcReqHeaderProtoBytes, err := proto.Marshal(&rpcReqHeaderProto)
	if err != nil {
		log.Fatal("proto.Marshal(&rpcReqHeaderProto)", err)
		return err
	}

	// 1. RequestHeaderProto
	requestHeaderProto := rpcCall.procedure
	requestHeaderProtoBytes, err := proto.Marshal(requestHeaderProto)
	if err != nil {
		log.Fatal("proto.Marshal(&requestHeaderProto)", err)
		return err
	}

	// 2. Param
	paramProto := rpcCall.request
	paramProtoBytes, err := proto.Marshal(paramProto)
	if err != nil {
		log.Fatal("proto.Marshal(&paramProto)", err)
		return err
	}

	totalLength := len(rpcReqHeaderProtoBytes) + sizeVarint(len(rpcReqHeaderProtoBytes)) + len(requestHeaderProtoBytes) + sizeVarint(len(requestHeaderProtoBytes)) + len(paramProtoBytes) + sizeVarint(len(paramProtoBytes))
	var tLen int32 = int32(totalLength)
	if totalLengthBytes, err := gohadoop.ConvertFixedToBytes(tLen); err != nil {
		log.Fatal("ConvertFixedToBytes(totalLength)", err)
		return err
	} else {
		if _, err := conn.con.Write(totalLengthBytes); err != nil {
			log.Fatal("conn.con.Write(totalLengthBytes)", err)
			return err
		}
	}

	if err := writeDelimitedBytes(conn, rpcReqHeaderProtoBytes); err != nil {
		log.Fatal("writeDelimitedBytes(conn, rpcReqHeaderProtoBytes)", err)
		return err
	}
	if err := writeDelimitedBytes(conn, requestHeaderProtoBytes); err != nil {
		log.Fatal("writeDelimitedBytes(conn, requestHeaderProtoBytes)", err)
		return err
	}
	if err := writeDelimitedBytes(conn, paramProtoBytes); err != nil {
		log.Fatal("writeDelimitedBytes(conn, paramProtoBytes)", err)
		return err
	}

	//log.Println("Succesfully sent request of length: ", totalLength)

	return nil
}

func writeDelimitedTo(conn *connection, msg proto.Message) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("proto.Marshal(msg)", err)
		return err
	}
	return writeDelimitedBytes(conn, msgBytes)
}

func writeDelimitedBytes(conn *connection, data []byte) error {
	if _, err := conn.con.Write(proto.EncodeVarint(uint64(len(data)))); err != nil {
		log.Fatal("conn.con.Write(proto.EncodeVarint(uint64(len(data))))", err)
		return err
	}
	if _, err := conn.con.Write(data); err != nil {
		log.Fatal("conn.con.Write(data)", err)
		return err
	}

	return nil
}

func (c *Client) readResponse(conn *connection, rpcCall *call) error {
	// Read first 4 bytes to get total-length
	var totalLength int32 = -1
	var totalLengthBytes [4]byte
	if _, err := conn.con.Read(totalLengthBytes[0:4]); err != nil {
		log.Fatal("conn.con.Read(totalLengthBytes)", err)
		return err
	}

	if err := gohadoop.ConvertBytesToFixed(totalLengthBytes[0:4], &totalLength); err != nil {
		log.Fatal("gohadoop.ConvertBytesToFixed(totalLengthBytes, &totalLength)", err)
		return err
	}

	var responseBytes []byte = make([]byte, totalLength)
	if _, err := conn.con.Read(responseBytes); err != nil {
		log.Fatal("conn.con.Read(totalLengthBytes)", err)
		return err
	}

	// Parse RpcResponseHeaderProto
	rpcResponseHeaderProto := hadoop_common.RpcResponseHeaderProto{}
	off, err := readDelimited(responseBytes[0:totalLength], &rpcResponseHeaderProto)
	if err != nil {
		log.Fatal("readDelimited(responseBytes, rpcResponseHeaderProto)", err)
		return err
	}
	//log.Println("Received rpcResponseHeaderProto = ", rpcResponseHeaderProto)

	err = c.checkRpcHeader(&rpcResponseHeaderProto)
	if err != nil {
		log.Fatal("c.checkRpcHeader failed", err)
		return err
	}

	if *rpcResponseHeaderProto.Status == hadoop_common.RpcResponseHeaderProto_SUCCESS {
		// Parse RpcResponseWrapper
		_, err = readDelimited(responseBytes[off:], rpcCall.response)
	} else {
		log.Println("RPC failed with status: ", rpcResponseHeaderProto.Status.String())
		errorDetails := [4]string{rpcResponseHeaderProto.Status.String(), "ServerDidNotSetExceptionClassName", "ServerDidNotSetErrorMsg", "ServerDidNotSetErrorDetail"}
		if rpcResponseHeaderProto.ExceptionClassName != nil {
			errorDetails[0] = *rpcResponseHeaderProto.ExceptionClassName
		}
		if rpcResponseHeaderProto.ErrorMsg != nil {
			errorDetails[1] = *rpcResponseHeaderProto.ErrorMsg
		}
		if rpcResponseHeaderProto.ErrorDetail != nil {
			errorDetails[2] = rpcResponseHeaderProto.ErrorDetail.String()
		}
		err = errors.New(strings.Join(errorDetails[:], ":"))
	}
	return err
}

func readDelimited(rawData []byte, msg proto.Message) (int, error) {
	headerLength, off := proto.DecodeVarint(rawData)
	if off == 0 {
		log.Fatal("proto.DecodeVarint(rawData) returned zero")
		return -1, nil
	}
	err := proto.Unmarshal(rawData[off:off+int(headerLength)], msg)
	if err != nil {
		log.Fatal("proto.Unmarshal(rawData[off:off+headerLength]) ", err)
		return -1, err
	}

	return off + int(headerLength), nil
}

func (c *Client) checkRpcHeader(rpcResponseHeaderProto *hadoop_common.RpcResponseHeaderProto) error {
	var callClientId [16]byte = [16]byte(*c.ClientId)
	var headerClientId []byte = []byte(rpcResponseHeaderProto.ClientId)
	if rpcResponseHeaderProto.ClientId != nil {
		if !bytes.Equal(callClientId[0:16], headerClientId[0:16]) {
			log.Fatal("Incorrect clientId: ", headerClientId)
			return errors.New("Incorrect clientId")
		}
	}
	return nil
}
