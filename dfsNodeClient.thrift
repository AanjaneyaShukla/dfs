exception InvalidAddress {
  1: i32 errorCode,
  2: string message
}

exception InvalidNode {
  1: i32 errorCode,
  2: string message
}

service DfsNodeClient {
        string join(1: string ip, 2: i32 port) throws (1:InvalidAddress excpt1, 2:InvalidNode excpt2),
        string write(1: string fileName, 2: string contents),
        string read(1: string fileName),
        string getDfsStructure(),
}
