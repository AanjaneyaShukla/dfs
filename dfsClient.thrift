struct FileContent {
  1: string contents,
  2: i32 version
}


service DfsClient {
        string write(1: string fileName, 2: string contents),
        string read(1: string fileName),
        i32 getVersion(1: string fileName),
        FileContent readCoordinator(1: string fileName),
        string writeCoordinator(1: string fileName, 2: string contents, 3: i32 version, 4:bool isSync),
        string getDfsStructure(),
        string getDfsStructureCoordinator(),
}