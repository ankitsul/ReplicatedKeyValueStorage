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

	private String destPort;

	private String predecessor1;
	private String successor1;
	private String predecessor2;
	private String successor2;

	List<PortHashObject> activePorts;

	boolean insertFlag;
	boolean queryFlag;
	boolean deleteFlag;

	boolean pingResponse;
	boolean queryResponseReceived;
	boolean allResponseReceived;

	private Cursor responseCursor;
	private MatrixCursor allResultCursor;
	private MatrixCursor partialResultCursor;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if (selection.equals("*")) {

		} else if (selection.equals("@")) {
			deleteLocalFiles();
		} else {
			deleteFlag = true;
			Log.v("delete", selection);
			// Sending delete request to the right node
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
		synchronized (this) {
			insertFlag = true;
			Log.v("insert", values.toString());

			// Sending Insert request to the right node
			serveRequest(values, null);
			try {
				Thread.sleep(150);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!pingResponse) {
				insertValues(values.getAsString(KEY_FIELD),
						values.getAsString(VALUE_FIELD),
						getSucessors(destPort), "0");
			}
			pingResponse = false;
			insertFlag = false;
		}
		return null;
	}

	/** Method to redirect the requests to the correct node */
	private void serveRequest(ContentValues values, String selection) {
		String filename = null;
		String value = null;
		if (insertFlag) {
			filename = values.getAsString(KEY_FIELD);
			value = values.getAsString(VALUE_FIELD);
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
						destPort = activePorts.get(i).getPortNumber();
						new ClientAsyncTask().executeOnExecutor(
								AsyncTask.THREAD_POOL_EXECUTOR, portStr,
								MessageType.INSERT.toString(), values
										.getAsString(KEY_FIELD), values
										.getAsString(VALUE_FIELD), activePorts
										.get(i).getPortNumber(), "2");
						break;
					} else if (queryFlag) {
						new ClientAsyncTask().executeOnExecutor(
								AsyncTask.THREAD_POOL_EXECUTOR, portStr,
								MessageType.QUERY_REQUEST.toString(),
								selection, "", activePorts.get(i)
										.getPortNumber());
						break;
					} else if (deleteFlag) {
						Log.d(TAG, "Inside delete 1");
						new ClientAsyncTask().executeOnExecutor(
								AsyncTask.THREAD_POOL_EXECUTOR, portStr,
								MessageType.DELETE_REQUEST.toString(),
								selection, "", activePorts.get(i)
										.getPortNumber(), "2");
						break;
					}

				} else if (i == 0
						&& (comparisonPredecessor < 0 || comparisonNode <= 0)) {
					Log.d(TAG, "Step 4");
					if (insertFlag) {
						pingResponse = true;
						insertLocally(filename, value);
						List<String> destinationPorts = Arrays.asList(
								activePorts.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						insertValues(values.getAsString(KEY_FIELD),
								values.getAsString(VALUE_FIELD),
								destinationPorts, "0");
						break;
					} else if (queryFlag) {
						new ClientAsyncTask().executeOnExecutor(
								AsyncTask.THREAD_POOL_EXECUTOR, portStr,
								MessageType.QUERY_REQUEST.toString(),
								selection, "", portStr);
						break;
					} else if (deleteFlag) {
						getContext().deleteFile(selection);
						List<String> destinationPorts = Arrays.asList(
								activePorts.get((i + 1) % 5).getPortNumber(),
								activePorts.get((i + 2) % 5).getPortNumber());
						deleteValues(selection, destinationPorts, "0");
						break;
					}

				}
			} else {
				if ((comparisonPredecessor < 0 && comparisonNode <= 0)
						|| (i == 0 && (comparisonPredecessor < 0 || comparisonNode <= 0))) {

					Log.d(TAG, "Step 6");
					if (insertFlag) {
						destPort = activePorts.get(i).getPortNumber();
						new ClientAsyncTask().executeOnExecutor(
								AsyncTask.THREAD_POOL_EXECUTOR, portStr,
								MessageType.INSERT.toString(), values
										.getAsString(KEY_FIELD), values
										.getAsString(VALUE_FIELD), activePorts
										.get(i).getPortNumber(), "2");
						break;
					} else if (queryFlag) {
						new ClientAsyncTask().executeOnExecutor(
								AsyncTask.THREAD_POOL_EXECUTOR, portStr,
								MessageType.QUERY_REQUEST.toString(),
								selection, "", activePorts.get(i)
										.getPortNumber());
						break;
					} else if (deleteFlag) {
						new ClientAsyncTask().executeOnExecutor(
								AsyncTask.THREAD_POOL_EXECUTOR, portStr,
								MessageType.DELETE_REQUEST.toString(),
								selection, "", activePorts.get(i)
										.getPortNumber(), "2");
						break;
					}
				}
			}
		}
	}

	/** Method to send insert request to multiple destination */
	private void insertValues(String key, String value,
			List<String> destinationPorts, String count) {
		for (int i = 0; i < destinationPorts.size(); i++) {
			new ClientAsyncTask().executeOnExecutor(
					AsyncTask.THREAD_POOL_EXECUTOR, portStr,
					MessageType.INSERT.toString(), key, value,
					destinationPorts.get(i), count);
		}
	}

	/** Method to send delete request to multiple destination */
	private void deleteValues(String selection, List<String> destinationPorts,
			String count) {
		for (int i = 0; i < destinationPorts.size(); i++) {
			new ClientAsyncTask().executeOnExecutor(
					AsyncTask.THREAD_POOL_EXECUTOR, portStr,
					MessageType.DELETE_REQUEST.toString(), selection, "",
					destinationPorts.get(i), count);
		}
	}

	/** Method to insert values locally */
	private void insertLocally(String filename, String value) {
		Log.d(TAG, "Value inserted locally:" + filename + ":" + value);
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
		pingResponse = false;

		responseCursor = null;

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

		syncData();
		return false;
	}

	/** Method to synchronize data with other nodes */
	private void syncData() {
		// Get data from predecessors which lies in their partition i.e.
		// non-replicated values, as in when they act as coordinators
		new DataSyncTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
				predecessor1, MessageType.SYNC_REQUEST_PREDECESSORS.toString(),
				null, null);
		new DataSyncTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
				predecessor2, MessageType.SYNC_REQUEST_PREDECESSORS.toString(),
				null, null);
		// Get data from successors which is supposed to be in this node (the
		// value might be replicated in the successors).
		new DataSyncTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
				successor1, MessageType.SYNC_REQUEST_SUCESSORS.toString(),
				predecessor1, successor1);
		new DataSyncTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
				successor2, MessageType.SYNC_REQUEST_SUCESSORS.toString(),
				predecessor1, successor1);
	}

	/** Server Async task to listen to request */
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
				e.printStackTrace();
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
				socket.close();
				// synchronized (this) {
				if (null != message) {
					Log.d(TAG, "Message Type:" + message.getMessageType() + ":"
							+ message.getSenderPort());
					if (message.getMessageType().equals(MessageType.INSERT)) {
						insertLocally(message.getKey(), message.getValue());
						if (message.getCount().equals("2")) {
							new ClientAsyncTask().executeOnExecutor(
									AsyncTask.THREAD_POOL_EXECUTOR, portStr,
									MessageType.INSERT.toString(),
									message.getKey(), message.getValue(),
									successor1, "0");
							new ClientAsyncTask().executeOnExecutor(
									AsyncTask.THREAD_POOL_EXECUTOR, portStr,
									MessageType.INSERT.toString(),
									message.getKey(), message.getValue(),
									successor2, "0");
							cursorClient(null, message.getSenderPort(),
									MessageType.PING_RESPONSE.toString());
						}

					} else if (message.getMessageType().equals(
							MessageType.QUERY_REQUEST)) {
						String[] columns = { KEY_FIELD, VALUE_FIELD };
						MatrixCursor cursor = new MatrixCursor(columns);
						Object[] row = new Object[cursor.getColumnCount()];

						StringBuffer value = getValue(message.getSelection());
						if (value != null) {
							row[cursor.getColumnIndex(KEY_FIELD)] = message
									.getSelection();
							row[cursor.getColumnIndex(VALUE_FIELD)] = value;
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
						Log.d(TAG, "Delete for" + message.getSelection());
						if (isFileAvailable(message.getSelection())) {
							getContext().deleteFile(message.getSelection());
						}
						if (message.getCount().equals("2")) {
							new ClientAsyncTask()
									.executeOnExecutor(
											AsyncTask.THREAD_POOL_EXECUTOR,
											portStr, MessageType.DELETE_REQUEST
													.toString(), message
													.getSelection(), "",
											successor1, "0");
							new ClientAsyncTask()
									.executeOnExecutor(
											AsyncTask.THREAD_POOL_EXECUTOR,
											portStr, MessageType.DELETE_REQUEST
													.toString(), message
													.getSelection(), "",
											successor2, "0");
						}
					} else if (message.getMessageType().equals(
							MessageType.QUERY_REQUEST_ALL)) {
						MatrixCursor localCursor = getLocalCursor();
						cursorClient(localCursor, message.getSenderPort(),
								MessageType.QUERY_RESPONSE_ALL.toString());
					} else if (message.getMessageType().equals(
							MessageType.QUERY_RESPONSE)) {
						responseCursor = convertMapToCursor(message
								.getCursorMap());
						queryResponseReceived = true;
					} else if (message.getMessageType().equals(
							MessageType.PING_RESPONSE)) {
						pingResponse = true;
					} else if (message.getMessageType().equals(
							MessageType.QUERY_RESPONSE_ALL)) {
						partialResultCursor = convertMapToCursor(message
								.getCursorMap());
						allResponseReceived = true;
					} else if (message.getMessageType().equals(
							MessageType.SYNC_REQUEST_PREDECESSORS)) {
						// Return data which lies in this partition (actual, not
						// replication)
						cursorClient(
								getSyncData(portStr, predecessor1, successor1),
								message.getSenderPort(),
								MessageType.SYNC_RESPONSE.toString());
					} else if (message.getMessageType().equals(
							MessageType.SYNC_REQUEST_SUCESSORS)) {
						// Return replicated data which is supposed to be in the
						// sender's node
						cursorClient(
								getSyncData(message.getSenderPort(),
										message.getSenderPredecessor(),
										message.getSenderSucessor()),
								message.getSenderPort(),
								MessageType.SYNC_RESPONSE.toString());
					} else if (message.getMessageType().equals(
							MessageType.SYNC_RESPONSE)) {
						MatrixCursor syncResponseCursor = convertMapToCursor(message
								.getCursorMap());
						while (syncResponseCursor.moveToNext()) {
							insertLocally(
									syncResponseCursor.getString(syncResponseCursor
											.getColumnIndex(KEY_FIELD)),
									syncResponseCursor.getString(syncResponseCursor
											.getColumnIndex(VALUE_FIELD)));
						}
					}
				}
				// }
			}
		}

		private MatrixCursor getSyncData(String node, String predecessor,
				String sucessor) {
			MatrixCursor localCursor = getLocalCursor();
			if (localCursor != null) {
				String[] columns = { KEY_FIELD, VALUE_FIELD };
				MatrixCursor syncCursor = new MatrixCursor(columns);
				Object[] row = new Object[syncCursor.getColumnCount()];
				while (localCursor.moveToNext()) {
					if (isInThisPartition(localCursor.getString(localCursor
							.getColumnIndex(KEY_FIELD)), node, predecessor,
							sucessor)) {
						// Add to the response packet
						row[syncCursor.getColumnIndex(KEY_FIELD)] = localCursor
								.getString(localCursor
										.getColumnIndex(KEY_FIELD));
						row[syncCursor.getColumnIndex(VALUE_FIELD)] = localCursor
								.getString(localCursor
										.getColumnIndex(VALUE_FIELD));
						syncCursor.addRow(row);
					}
				}
				syncCursor.close();
				return syncCursor;
			} else {
				return null;
			}
		}

		private boolean isInThisPartition(String key, String node,
				String predecessor, String successor) {
			try {
				int comparisonPredecessor = genHash(predecessor).compareTo(
						genHash(key));
				int comparisonNode = genHash(key).compareTo(genHash(node));
				if ((comparisonPredecessor < 0 && comparisonNode < 0)
						|| ((node.equals("5562")) && (comparisonPredecessor < 0 || comparisonNode < 0))) {
					return true;
				}
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG,
						"No such algorithm exception while creating SHA1 hash:"
								+ e.getMessage());
			}
			return false;
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
						predecessor1 = activePorts.get(activePorts.size() - 1)
								.getPortNumber();
						predecessor2 = activePorts.get(activePorts.size() - 2)
								.getPortNumber();
						successor1 = activePorts.get(i + 1).getPortNumber();
						successor2 = activePorts.get(i + 2).getPortNumber();
					} else if (i == 1) {
						predecessor1 = activePorts.get(0).getPortNumber();
						predecessor2 = activePorts.get(activePorts.size() - 1)
								.getPortNumber();
						successor1 = activePorts.get(i + 1).getPortNumber();
						successor2 = activePorts.get(i + 2).getPortNumber();
					} else if (i == (activePorts.size() - 1)) {
						predecessor1 = activePorts.get(i - 1).getPortNumber();
						predecessor2 = activePorts.get(i - 2).getPortNumber();
						successor1 = activePorts.get(0).getPortNumber();
						successor2 = activePorts.get(1).getPortNumber();
					} else if (i == (activePorts.size() - 2)) {
						predecessor1 = activePorts.get(i - 1).getPortNumber();
						predecessor2 = activePorts.get(i - 2).getPortNumber();
						successor1 = activePorts.get(activePorts.size() - 1)
								.getPortNumber();
						successor2 = activePorts.get(0).getPortNumber();
					} else {
						predecessor1 = activePorts.get(i - 1).getPortNumber();
						predecessor2 = activePorts.get(i - 2).getPortNumber();
						successor1 = activePorts.get(i + 1).getPortNumber();
						successor2 = activePorts.get(i + 2).getPortNumber();
					}
				}
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "No such algorithm exception while creating SHA1 hash:"
					+ e.getMessage());
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		Log.v(TAG, "query:" + selection);
		String[] columns = { KEY_FIELD, VALUE_FIELD };
		StringBuffer value = new StringBuffer();
		MatrixCursor cursor = new MatrixCursor(columns);
		Object[] row = new Object[cursor.getColumnCount()];
		if (selection.equals("*")) {
			allResultCursor = getLocalCursor();
			for (int i = 0; i < activePorts.size(); i++) {
				if (activePorts.get(i).getPortNumber() != portStr) {
					new ClientAsyncTask().executeOnExecutor(
							AsyncTask.THREAD_POOL_EXECUTOR, portStr,
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
			return getLocalCursor();
		} else {
			synchronized (this) {
				if (isFileAvailable(selection)) {
					Log.d(TAG, "Stepp 1");
					value = getValue(selection);
					Log.d(TAG, "Stepp 6" + value);
					row[cursor.getColumnIndex(KEY_FIELD)] = selection;
					row[cursor.getColumnIndex(VALUE_FIELD)] = value;
					cursor.addRow(row);
					cursor.close();
					return cursor;
				} else if (!isFileAvailable(selection)) {
					Log.d(TAG, "Stepp 2");
					queryFlag = true;
					while (responseCursor == null) {
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							Log.e(TAG,
									"Interrupted exception while thread sleep:"
											+ e.getMessage());
						}
						serveRequest(null, selection);
						while (!queryResponseReceived) {
							// Wait until the response is received
						}
						Log.d(TAG, "@@@Out of loop, Response Cursor:"
								+ responseCursor);
						queryResponseReceived = false;
					}
					Log.d(TAG, "@@@Out of bigger loop, Response Cursor:"
							+ responseCursor);
					;
					// resetting it to false
					queryFlag = false;
					Cursor returnCursor = responseCursor;
					responseCursor = null;
					return returnCursor;
				}
			}
		}
		return null;
	}

	/** Method to concatenate the result cursor with the partial result */
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
	private MatrixCursor getLocalCursor() {
		String[] columns = { KEY_FIELD, VALUE_FIELD };
		MatrixCursor cursor = new MatrixCursor(columns);
		Object[] row = new Object[cursor.getColumnCount()];
		StringBuffer value;
		File[] files = getAllFiles();
		if (files != null) {
			for (int i = 0; i < files.length; i++) {
				value = getValue(files[i].getName());
				Log.d(TAG, "KEY,VALUE:" + files[i] + ":" + value);
				row[cursor.getColumnIndex(KEY_FIELD)] = files[i].getName();
				row[cursor.getColumnIndex(VALUE_FIELD)] = value;
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
			Log.e(TAG,
					"IO Exception while retrieving data from file:"
							+ e.getMessage());
		}
		return value;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	/** Client Async task to send request to other nodes */
	private class ClientAsyncTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
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
				message.setCount(msgs[5]);
				Log.d(TAG,
						"Insert request sending to the " + destination + "Key:"
								+ message.getKey() + "Value:"
								+ message.getValue());
			} else if (message.getMessageType().equals(
					MessageType.QUERY_REQUEST)) {
				message.setSelection(msgs[2]);
			} else if (message.getMessageType().equals(
					MessageType.DELETE_REQUEST)) {
				message.setSelection(msgs[2]);
				message.setCount(msgs[5]);
			}

			Log.d(TAG, "Sent Request type:" + message.getMessageType() + "Key:"
					+ message.getKey() + "Value:" + message.getValue()
					+ "Selection:" + message.getSelection() + "Sending to:"
					+ destination + " Count:" + message.getCount());

			try {
				socket = new Socket(InetAddress.getByAddress(new byte[] { 10,
						0, 2, 2 }), Integer.parseInt(destination) * 2);
				objectOutputStream = new ObjectOutputStream(
						socket.getOutputStream());
				objectOutputStream.writeObject(message);
				objectOutputStream.close();
				socket.close();
			} catch (IOException e) {
				Log.e(TAG,
						"IO Exception while creating socket:" + e.getMessage());
				List<String> sucessors = getSucessors(destination);
				if (message.getMessageType().equals(MessageType.INSERT)
						&& (message.getCount().equals("2"))) {
					Log.e(TAG, "Insert exception");
					insertValues(message.getKey(), message.getValue(),
							sucessors, "0");
				} else if (message.getMessageType().equals(
						MessageType.QUERY_REQUEST)) {
					Log.e(TAG, "Query exception");
					new ClientAsyncTask().executeOnExecutor(
							AsyncTask.THREAD_POOL_EXECUTOR, portStr,
							MessageType.QUERY_REQUEST.toString(),
							message.getSelection(), "", sucessors.get(0));
				} else if (message.getMessageType().equals(
						MessageType.DELETE_REQUEST)) {
					Log.e(TAG, "Delete exception");
					deleteValues(message.getSelection(), sucessors, "0");
				} else if ((message.getMessageType()
						.equals(MessageType.QUERY_REQUEST_ALL))
						|| (message.getMessageType()
								.equals(MessageType.DELETE_REQUEST_ALL))) {
					Log.e(TAG, "Query All exception");
					allResponseReceived = true;
				}
			}
			return null;
		}
	}

	/** Method to get successors of a node */
	private List<String> getSucessors(String destination) {
		List<String> sucessors = new ArrayList<String>();
		for (int i = 0; i < activePorts.size(); i++) {
			if ((destination.equals(activePorts.get(i).getPortNumber()))
					&& (i == (activePorts.size() - 1))) {
				sucessors.add(activePorts.get(0).getPortNumber());
				sucessors.add(activePorts.get(1).getPortNumber());
				break;
			} else if ((destination.equals(activePorts.get(i).getPortNumber()))
					&& (i == (activePorts.size() - 2))) {
				sucessors.add(activePorts.get(activePorts.size() - 1)
						.getPortNumber());
				sucessors.add(activePorts.get(0).getPortNumber());
				break;
			} else if (destination.equals(activePorts.get(i).getPortNumber())) {
				sucessors.add(activePorts.get(i + 1).getPortNumber());
				sucessors.add(activePorts.get(i + 2).getPortNumber());
				break;
			}
		}
		return sucessors;
	}

	/** Client Async task to send the response back to the requester */
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

	/** Async task to synchronize data from other nodes */
	private class DataSyncTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			Socket socket = null;
			ObjectOutputStream objectOutputStream;
			Message message = new Message();
			message.setSenderPort(portStr);
			message.setSenderPredecessor(msgs[2]);
			message.setSenderSucessor(msgs[3]);

			// Setting the type of request
			message.setMessageType(MessageType.valueOf(msgs[1]));
			try {
				Log.d(TAG,
						"@@@@SYNC request sent Request type:"
								+ message.getMessageType() + "Sending to:"
								+ msgs[0]);
				socket = new Socket(InetAddress.getByAddress(new byte[] { 10,
						0, 2, 2 }), Integer.parseInt(msgs[0]) * 2);
				objectOutputStream = new ObjectOutputStream(
						socket.getOutputStream());
				objectOutputStream.writeObject(message);
				objectOutputStream.close();
				socket.close();
			} catch (IOException e) {
				Log.e(TAG,
						"IO Exception while creating socket:" + e.getMessage());
			}
			return null;
		}
	}

	/** Method to convert Cursor to a Map */
	private Map<String, String> convertCursorToMap(Cursor cursor) {
		if (cursor == null) {
			return null;
		}
		Map<String, String> cursorMap = new HashMap<String, String>();
		while (cursor.moveToNext()) {
			cursorMap.put(cursor.getString(cursor.getColumnIndex(KEY_FIELD)),
					cursor.getString(cursor.getColumnIndex(VALUE_FIELD)));
		}
		return cursorMap;
	}

	/** Method to convert Map into a Cursor object */
	private MatrixCursor convertMapToCursor(Map<String, String> cursorMap) {
		if (cursorMap == null) {
			return null;
		}
		String[] columns = { KEY_FIELD, VALUE_FIELD };
		MatrixCursor cursor = new MatrixCursor(columns);
		for (Map.Entry<String, String> entry : cursorMap.entrySet()) {
			Object[] row = new Object[cursor.getColumnCount()];
			row[cursor.getColumnIndex(KEY_FIELD)] = entry.getKey();
			row[cursor.getColumnIndex(VALUE_FIELD)] = entry.getValue();
			cursor.addRow(row);
		}
		cursor.close();
		return cursor;
	}

	/** Method to generate SHA-1 hash */
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