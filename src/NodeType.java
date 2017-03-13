
public enum NodeType {

	Coordinator("coordinator"), Node("node");

	private String text;

	NodeType(String text) {
		this.text = text;
	}

	public static NodeType convertStrToEnum(String text) {
		if (text != null) {
			for (NodeType node : NodeType.values()) {
				if (text.equalsIgnoreCase(node.text))
					return node;
			}
		}
		return null;
	}
}
