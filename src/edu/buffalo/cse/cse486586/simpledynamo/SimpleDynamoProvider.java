package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;

	/* Constants for content provider table columns */
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private String portStr;
	private String myPort;

	private String predecessor;
	private String successor;

	List<PortHashObject> activePorts;

	boolean insertFlag;
	boolean queryFlag;
	boolean deleteFlag;
	boolean queryResponseReceived;
	boolean allResponseReceived;

	private Cursor responseCursor;
	private MatrixCursor allResultCursor;
	private MatrixCursor partialResultCursor;

	// private Lock opeartionLock;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if (selection.equals("*")) {

		} else if (selection.equals("@")) {
			deleteLocalFiles();
		} else {
			deleteFlag = true;
			Log.v("delete", selection);
			// Sending Insert request to the right node
			serveRequest(null, selection);
			deleteFlag = false;
		}
		return 0;
	}

	/** Method to delete all files from the content provider */
	private void deleteLocalFiles() {
		File[] files = getAllFiles();
		if (files != null) {
			for (int i = 0; i < files.length; i++) {
				getContext().deleteFile(files[i].toString());
			}
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// opeartionLock.lock();
		synchronized (this) {
			insertFlag = true;
			Log.v("insert", values.toString());

			// Sending Insert request to the right node
			serveRequest(values, null);
			insertFlag = false;
		}
		// opeartionLock.unlock();
		return null;
	}

	private void serveRequest(ContentValues values, String selection) {
		String filename = null;
		String value = null;
		if (insertFlag) {
			filename = values.getAsString("key");
			value = values.getAsString("value");
		} else if (queryFlag || deleteFlag) {
			filename = selection;
		}

		for (int i = 0; i < activePorts.size(); i++) {
			int comparisonPredecessor = 0;
			int comparisonNode = 0;
			try {
				if (i != 0) {
					comparisonPredecessor = genHash(
							activePorts.get(i - 1).getPortNumber()).compareTo(
							genHash(filename));
				} else {
					comparisonPredecessor = genHash(
							activePorts.get(activePorts.size() - 1)
									.getPortNumber()).compareTo(
							genHash(filename));
				}
				comparisonNode = genHash(filename).compareTo(
						genHash(activePorts.get(i).getPortNumber()));
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG,
						"No such algorithm exception while creating SHA1 hash:"
								+ e.getMessage());
			}
			// To handle the wrapped DHT
			if (portStr.equals(activePorts.get(0).getPortNumber())) {
				if (comparisonPredecessor < 0 && comparisonNode <= 0) {
					Log.d(TAG, "Step 3");
					if (insertFlag) {
						List<String> destinationPorts = Arrays.asList(
								activePorts.get(i).getPortNumber(), activePorts
										.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						insertValues(values, destinationPorts);
						break;
					} else if (queryFlag) {
						// synchronized (this) {
						sendRequest(portStr,
								MessageType.QUERY_REQUEST.toString(),
								selection, "", activePorts.get(i)
										.getPortNumber());
						// }
					} else if (deleteFlag) {
						List<String> destinationPorts = Arrays.asList(
								activePorts.get(i).getPortNumber(), activePorts
										.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						deleteValues(selection, destinationPorts);
						break;
					}

				} else if (i == 0
						&& (comparisonPredecessor < 0 || comparisonNode <= 0)) {
					Log.d(TAG, "Step 4");
					if (insertFlag) {
						insertLocally(filename, value);
						List<String> destinationPorts = Arrays.asList(
								activePorts.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						insertValues(values, destinationPorts);
						break;
					} else if (queryFlag) {
						// synchronized (this) {
						sendRequest(portStr,
								MessageType.QUERY_REQUEST.toString(),
								selection, "", portStr);
						// }
					} else if (deleteFlag) {
						getContext().deleteFile(selection);
						List<String> destinationPorts = Arrays.asList(
								activePorts.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						deleteValues(selection, destinationPorts);
						break;
					}

				}
			} else {
				if ((comparisonPredecessor < 0 && comparisonNode <= 0)
						|| (i == 0 && (comparisonPredecessor < 0 || comparisonNode <= 0))) {

					Log.d(TAG, "Step 6");
					if (insertFlag) {
						List<String> destinationPorts = Arrays.asList(
								activePorts.get(i).getPortNumber(), activePorts
										.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						insertValues(values, destinationPorts);
						break;
					} else if (queryFlag) {
						// synchronized (this) {
						sendRequest(portStr,
								MessageType.QUERY_REQUEST.toString(),
								selection, "", activePorts.get(i)
										.getPortNumber());
						// }
					} else if (deleteFlag) {
						List<String> destinationPorts = Arrays.asList(
								activePorts.get(i).getPortNumber(), activePorts
										.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						deleteValues(selection, destinationPorts);
						break;
					}
				}
			}
		}
	}

	private void insertValues(ContentValues values,
			List<String> destinationPorts) {
		for (int i = 0; i < destinationPorts.size(); i++) {
			sendRequest(portStr, MessageType.INSERT.toString(),
					values.getAsString("key"), values.getAsString("value"),
					destinationPorts.get(i));
		}
	}

	private void deleteValues(String selection, List<String> destinationPorts) {
		for (int i = 0; i < destinationPorts.size(); i++) {
			sendRequest(portStr, MessageType.DELETE_REQUEST.toString(),
					selection, "", destinationPorts.get(i));
		}
	}

	private void insertLocally(String filename, String value) {
		Log.d(TAG, "@@@Value inserted locally:" + filename + ":" + value);
		try {
			FileOutputStream fileOutputStream = getContext().openFileOutput(
					filename, Context.MODE_PRIVATE);
			fileOutputStream.write(value.getBytes());
			fileOutputStream.close();
		} catch (IOException e) {
			Log.e(TAG,
					"IO Exception while writing to the file:" + e.getMessage());
		}
	}

	@Override
	public boolean onCreate() {
		queryResponseReceived = false;
		allResponseReceived = false;

		responseCursor = null;

		// opeartionLock = new ReentrantLock(true);

		TelephonyManager tel = (TelephonyManager) this.getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);

		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		activePorts = new ArrayList<PortHashObject>();
		arrangeNodes();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
					serverSocket);
		} catch (IOException e) {
			Log.e(TAG,
					"IO Exception while creating server socket:"
							+ e.getMessage());
		}
		return false;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				readMessage(serverSocket);
			} catch (IOException e) {
				Log.e(TAG,
						"IO Exception while reading the message from the stream:"
								+ e.getMessage());
			} catch (ClassNotFoundException e) {
				Log.e(TAG,
						"Class loader is unable to load the class:"
								+ e.getMessage());
			}
			return null;
		}

		private void readMessage(ServerSocket serverSocket) throws IOException,
				ClassNotFoundException {
			Message message = null;
			while (true) {
				Socket socket = serverSocket.accept();
				ObjectInputStream objectInputStream = new ObjectInputStream(
						socket.getInputStream());
				message = (Message) objectInputStream.readObject();
				objectInputStream.close();
				if (null != message) {
					Log.d(TAG, "@@@Message Type:" + message.getMessageType());
					if (message.getMessageType().equals(MessageType.INSERT)) {
						insertLocally(message.getKey(), message.getValue());
					} else if (message.getMessageType().equals(
							MessageType.QUERY_REQUEST)) {
						String[] columns = { "key", "value" };
						MatrixCursor cursor = new MatrixCursor(columns);
						Object[] row = new Object[cursor.getColumnCount()];

						StringBuffer value = getValue(message.getSelection());
						if (value != null) {
							row[cursor.getColumnIndex("key")] = message
									.getSelection();
							row[cursor.getColumnIndex("value")] = value;
							cursor.addRow(row);
							cursor.close();
							cursorClient(cursor, message.getSenderPort(),
									MessageType.QUERY_RESPONSE.toString());
						} else {
							cursorClient(null, message.getSenderPort(),
									MessageType.QUERY_RESPONSE.toString());
						}

					} else if (message.getMessageType().equals(
							MessageType.DELETE_REQUEST)) {
						getContext().deleteFile(message.getSelection());
					} else if (message.getMessageType().equals(
							MessageType.QUERY_REQUEST_ALL)) {
						String[] columns = { "key", "value" };
						MatrixCursor localCursor = getLocalCursor(columns);
						cursorClient(localCursor, message.getSenderPort(),
								MessageType.QUERY_RESPONSE_ALL.toString());
					} else if (message.getMessageType().equals(
							MessageType.QUERY_RESPONSE)) {
						responseCursor = convertMapToCursor(message
								.getCursorMap());
						queryResponseReceived = true;
					} else if (message.getMessageType().equals(
							MessageType.QUERY_RESPONSE_ALL)) {
						partialResultCursor = convertMapToCursor(message
								.getCursorMap());
						allResponseReceived = true;
					}
				}
			}
		}

		/** Method to convert Map into a Cursor object */
		private MatrixCursor convertMapToCursor(Map<String, String> cursorMap) {
			if (cursorMap == null) {
				return null;
			}
			String[] columns = { "key", "value" };
			MatrixCursor cursor = new MatrixCursor(columns);
			for (Map.Entry<String, String> entry : cursorMap.entrySet()) {
				Object[] row = new Object[cursor.getColumnCount()];

				row[cursor.getColumnIndex("key")] = entry.getKey();
				row[cursor.getColumnIndex("value")] = entry.getValue();

				cursor.addRow(row);
			}
			cursor.close();
			return cursor;
		}
	}

	private void arrangeNodes() {
		try {
			PortHashObject portHashObject = new PortHashObject("5554",
					genHash("5554"));
			activePorts.add(portHashObject);
			portHashObject = new PortHashObject("5556", genHash("5556"));
			activePorts.add(portHashObject);
			portHashObject = new PortHashObject("5558", genHash("5558"));
			activePorts.add(portHashObject);
			portHashObject = new PortHashObject("5560", genHash("5560"));
			activePorts.add(portHashObject);
			portHashObject = new PortHashObject("5562", genHash("5562"));
			activePorts.add(portHashObject);
			Collections.sort(activePorts);

			for (int i = 0; i < activePorts.size(); i++) {
				if (portStr.equals(activePorts.get(i).getPortNumber())) {
					if (i == 0) {
						predecessor = activePorts.get(activePorts.size() - 1)
								.getPortNumber();
						successor = activePorts.get(i + 1).getPortNumber();
					} else if (i == (activePorts.size() - 1)) {
						predecessor = activePorts.get(i - 1).getPortNumber();
						successor = activePorts.get(0).getPortNumber();
					} else {
						predecessor = activePorts.get(i - 1).getPortNumber();
						successor = activePorts.get(i + 1).getPortNumber();
					}
				}
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// opeartionLock.lock();

		/*
		 * while (insertFlag) {
		 * 
		 * }
		 */

		/*
		 * try { Thread.sleep(100); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */
		Log.v(TAG, "query:" + selection);
		String[] columns = { "key", "value" };
		StringBuffer value = new StringBuffer();
		MatrixCursor cursor = new MatrixCursor(columns);
		Object[] row = new Object[cursor.getColumnCount()];
		if (selection.equals("*")) {
			allResultCursor = getLocalCursor(columns);
			for (int i = 0; i < activePorts.size(); i++) {
				if (activePorts.get(i).getPortNumber() != portStr) {
					sendRequest(portStr,
							MessageType.QUERY_REQUEST_ALL.toString(), "", "",
							activePorts.get(i).getPortNumber());
					while (!allResponseReceived) {

					}
					allResponseReceived = false;
					allResultCursor = concat(allResultCursor,
							partialResultCursor);
				}
			}
			return allResultCursor;
		} else if (selection.equals("@")) {
			Log.v(TAG, "query1:" + selection);
			return getLocalCursor(columns);
		} else {
			if (isFileAvailable(selection)) {
				value = getValue(selection);
				row[cursor.getColumnIndex("key")] = selection;
				row[cursor.getColumnIndex("value")] = value;
				cursor.addRow(row);
				cursor.close();
				return cursor;
			} else if (!isFileAvailable(selection)) {
				synchronized (this) {
					queryFlag = true;
					while (responseCursor == null) {
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						serveRequest(null, selection);
						while (!queryResponseReceived) {
							// Wait until the response is received
						}
						queryResponseReceived = false;
					}
					// resetting it to false
					queryFlag = false;
					Cursor returnCursor = responseCursor;
					responseCursor = null;
					return returnCursor;
				}
			}
		}
		// opeartionLock.unlock();
		return null;
	}

	private MatrixCursor concat(MatrixCursor allResultCursor,
			MatrixCursor partialResultCursor) {
		if (null == partialResultCursor) {
			return allResultCursor;
		}

		Map<String, String> allResultMap = convertCursorToMap(allResultCursor);
		Map<String, String> partialResultMap = convertCursorToMap(partialResultCursor);

		for (Map.Entry<String, String> entry : partialResultMap.entrySet()) {
			allResultMap.put(entry.getKey(), entry.getValue());
		}
		MatrixCursor result = convertMapToCursor(allResultMap);
		return result;
	}

	/** Method to check if the requested key is present in the content provider */
	private boolean isFileAvailable(String filename) {
		File file = getContext().getFileStreamPath(filename);
		return file.exists();
	}

	/**
	 * Method to retrieve the cursor object containing all the data from the
	 * content provider
	 */
	private MatrixCursor getLocalCursor(String[] columns) {
		Log.v(TAG, "query2:");
		MatrixCursor cursor = new MatrixCursor(columns);
		Object[] row = new Object[cursor.getColumnCount()];
		StringBuffer value;
		File[] files = getAllFiles();
		if (files != null) {
			for (int i = 0; i < files.length; i++) {
				value = getValue(files[i].getName());
				Log.d(TAG, "KEY,VALUE:" + files[i] + ":" + value);

				row[cursor.getColumnIndex("key")] = files[i].getName();
				row[cursor.getColumnIndex("value")] = value;
				cursor.addRow(row);
			}
			cursor.close();
		}
		return cursor;
	}

	/** Method to retrieve all the files from the content provider */
	private File[] getAllFiles() {
		File currentDir = new File(System.getProperty("user.dir")
				+ "data/data/edu.buffalo.cse.cse486586.simpledynamo/files");
		return currentDir.listFiles();
	}

	/** Method to retrieve the value corresponding to the requested key */
	private StringBuffer getValue(String selection) {
		int ch;
		StringBuffer value = new StringBuffer();
		FileInputStream fileInputStream;
		try {
			fileInputStream = getContext().openFileInput(selection);

			while ((ch = fileInputStream.read()) != -1) {
				value.append((char) ch);
			}
			fileInputStream.close();
		} catch (FileNotFoundException e) {
			Log.e(TAG, "File not found exception:" + e.getMessage());
			return null;
		} catch (IOException e) {
			Log.e(TAG, "IO Exception while creating socket:" + e.getMessage());
		}
		return value;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private void sendRequest(String... msgs) {
		Socket socket = null;
		String destination = null;
		ObjectOutputStream objectOutputStream;
		Message message = new Message();
		message.setSenderPort(msgs[0]);
		destination = msgs[4];

		// Setting the type of request
		message.setMessageType(MessageType.valueOf(msgs[1]));

		if (message.getMessageType().equals(MessageType.INSERT)) {
			message.setKey(msgs[2]);
			message.setValue(msgs[3]);
			Log.d(TAG, "@@@Insert request sending to the " + destination);
		} else if ((message.getMessageType().equals(MessageType.QUERY_REQUEST))
				|| (message.getMessageType().equals(MessageType.DELETE_REQUEST))) {
			message.setSelection(msgs[2]);
		}

		try {
			socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2,
					2 }), Integer.parseInt(destination) * 2);
			objectOutputStream = new ObjectOutputStream(
					socket.getOutputStream());
			objectOutputStream.writeObject(message);
			objectOutputStream.close();
			socket.close();
		} catch (IOException e) {
			Log.e(TAG, "IO Exception while creating socket:" + e.getMessage());
		}
	}

	public void cursorClient(Cursor cursor, String senderPort,
			String messageType) {
		Socket socket = null;
		ObjectOutputStream objectOutputStream;
		String destinationPort = senderPort;

		Message message = new Message();
		Map<String, String> cursorMap = null;
		cursorMap = convertCursorToMap(cursor);
		message.setCursorMap(cursorMap);
		message.setSenderPort(portStr);
		message.setMessageType(MessageType.valueOf(messageType));

		try {
			socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2,
					2 }), Integer.parseInt(destinationPort) * 2);

			objectOutputStream = new ObjectOutputStream(
					socket.getOutputStream());
			objectOutputStream.writeObject(message);
			objectOutputStream.close();
		} catch (IOException e) {
			Log.e(TAG, "IO Exception while creating socket:" + e.getMessage());
		}
	}

	/** Method to convert Cursor to a Map */
	private Map<String, String> convertCursorToMap(Cursor cursor) {
		if (cursor == null) {
			return null;
		}
		Map<String, String> cursorMap = new HashMap<String, String>();
		while (cursor.moveToNext()) {
			cursorMap.put(cursor.getString(cursor.getColumnIndex("key")),
					cursor.getString(cursor.getColumnIndex("value")));
		}
		return cursorMap;
	}

	/** Method to convert Map into a Cursor object */
	private MatrixCursor convertMapToCursor(Map<String, String> cursorMap) {
		String[] columns = { "key", "value" };
		MatrixCursor cursor = new MatrixCursor(columns);
		for (Map.Entry<String, String> entry : cursorMap.entrySet()) {
			Object[] row = new Object[cursor.getColumnCount()];

			row[cursor.getColumnIndex("key")] = entry.getKey();
			row[cursor.getColumnIndex("value")] = entry.getValue();

			cursor.addRow(row);
		}
		cursor.close();
		return cursor;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
