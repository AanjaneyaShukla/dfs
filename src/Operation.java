
public class Operation {

	public NodeOperations op;
	public String fileName;
	public String contents;
	
	public Operation(NodeOperations op, String fileName, String contents) {
		this.op = op;
		this.fileName = fileName;
		this.contents = contents;
	}

	@Override
	public String toString() {
		return "Operation [op=" + op + ", fileName=" + fileName + ", contents=" + contents + "]";
	}

	public NodeOperations getOp() {
		return op;
	}

	public void setOp(NodeOperations op) {
		this.op = op;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getContents() {
		return contents;
	}

	public void setContents(String contents) {
		this.contents = contents;
	}
	
	
}
