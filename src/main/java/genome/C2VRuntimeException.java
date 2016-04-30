package genome;

@SuppressWarnings("serial")
public class C2VRuntimeException extends RuntimeException {
	public C2VRuntimeException(Exception e) {
		super(e);
	}

	public C2VRuntimeException(String message, Exception e) {
		super(message, e);
	}

	public C2VRuntimeException(String message) {
		super(message);
	}
}