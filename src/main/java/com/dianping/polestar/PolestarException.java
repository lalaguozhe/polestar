package com.dianping.polestar;

public class PolestarException extends Exception {
	private static final long serialVersionUID = 6525L;

	public PolestarException() {
		super();
	}

	public PolestarException(String message) {
		super(message);
	}

	public PolestarException(Throwable e) {
		super(e);
	}

	public PolestarException(String msg, Throwable e) {
		super(msg, e);
	}
}
