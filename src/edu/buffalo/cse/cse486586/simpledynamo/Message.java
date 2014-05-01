package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import android.database.Cursor;

public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	MessageType messageType;
	String senderPort;

	/* For insert operation */
	String key;
	String value;

	/* For query request */
	String selection;

	String senderPredecessor;
	String senderSucessor;

	String count;

	String responsePort;

	public String getResponsePort() {
		return responsePort;
	}

	public void setResponsePort(String responsePort) {
		this.responsePort = responsePort;
	}

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	boolean firstRequest;

	public String getSenderPredecessor() {
		return senderPredecessor;
	}

	public void setSenderPredecessor(String senderPredecessor) {
		this.senderPredecessor = senderPredecessor;
	}

	public String getSenderSucessor() {
		return senderSucessor;
	}

	public void setSenderSucessor(String senderSucessor) {
		this.senderSucessor = senderSucessor;
	}

	/* Required for cursor response */
	Map<String, String> cursorMap;

	public boolean isFirstRequest() {
		return firstRequest;
	}

	public void setFirstRequest(boolean firstRequest) {
		this.firstRequest = firstRequest;
	}

	public Map<String, String> getCursorMap() {
		return cursorMap;
	}

	public void setCursorMap(Map<String, String> cursorMap) {
		this.cursorMap = cursorMap;
	}

	public String getSelection() {
		return selection;
	}

	public void setSelection(String selection) {
		this.selection = selection;
	}

	public MessageType getMessageType() {
		return messageType;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}

	public String getSenderPort() {
		return senderPort;
	}

	public void setSenderPort(String senderPort) {
		this.senderPort = senderPort;
	}
}
