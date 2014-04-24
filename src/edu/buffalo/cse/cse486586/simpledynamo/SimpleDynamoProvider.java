package edu.buffalo.cse.cse486586.simpledynamo;

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
import java.util.Collections;
import java.util.Formatter;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
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

	private String successor;

	List<PortHashObject> activePorts;

	boolean insertCarryOver;
	int currentInsertCount;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.v("insert", values.toString());
		String filename = null;
		filename = values.getAsString("key");

		String value = values.getAsString("value");

		if (insertCarryOver) {
			Log.d(TAG, "@@@Inside carry over");
			insertLocally(filename, value);
			sendInsertRequest(portStr, MessageType.INSERT.toString(),
					values.getAsString("key"), values.getAsString("value"),
					successor, Integer.toString(currentInsertCount - 1));
			insertCarryOver = false;
		} else {
			Log.d(TAG, "@@@New request - not carry over");
			for (int i = 0; i < activePorts.size(); i++) {
				Log.d(TAG, "@@@Port:" + activePorts.get(i).getPortNumber());
				int comparisonPredecessor = 0;
				int comparisonNode = 0;
				try {
					if (i != 0) {
						
						comparisonPredecessor = genHash(
								activePorts.get(i - 1).getPortNumber())
								.compareTo(genHash(filename));
						Log.d(TAG,
								"@@@Not first position in the active ports array" + activePorts.get(i-1).getPortNumber() + ":" + comparisonPredecessor);
					} else {
						
						comparisonPredecessor = genHash(
								activePorts.get(activePorts.size() - 1)
										.getPortNumber()).compareTo(
								genHash(filename));
						Log.d(TAG,
								"@@@First position in the active ports array" + activePorts.get(activePorts.size() - 1).getPortNumber() + ":" + comparisonPredecessor);
					}

					comparisonNode = genHash(filename).compareTo(
							genHash(activePorts.get(i).getPortNumber()));
					Log.d(TAG,
							"@@@Comparison node" + comparisonNode);
				} catch (NoSuchAlgorithmException e) {
					Log.e(TAG,
							"No such algorithm exception while creating SHA1 hash:"
									+ e.getMessage());
				}

				if (portStr.equals(activePorts.get(0).getPortNumber())) {
					Log.d(TAG, "@@@First position in the active ports array #" + comparisonPredecessor + ":" + comparisonNode);
					if (comparisonPredecessor < 0 && comparisonNode >= 0) {
						
					}
					else if (comparisonPredecessor < 0 && comparisonNode <= 0) {
						sendInsertRequest(portStr,
								MessageType.INSERT.toString(),
								values.getAsString("key"),
								values.getAsString("value"), activePorts.get(i)
								.getPortNumber(), Integer.toString(3));
					}
			
					else if (comparisonPredecessor < 0 || comparisonNode <= 0) {
						insertLocally(filename, value);
						sendInsertRequest(portStr,
								MessageType.INSERT.toString(),
								values.getAsString("key"),
								values.getAsString("value"), successor,
								Integer.toString(2));
					} 

				} else {
					Log.d(TAG,
							"@@@Inside else: forwarding the request to approriate");
					if (comparisonPredecessor < 0 && comparisonNode <= 0) {
						Log.d(TAG,
								"@@@Inside else: forwarding the request to approriate#");
						sendInsertRequest(portStr,
								MessageType.INSERT.toString(),
								values.getAsString("key"),
								values.getAsString("value"), activePorts.get(i)
										.getPortNumber(), Integer.toString(3));
						// insertLocally(filename, value);
						// sendInsertRequest(filename, value, successor, 2);
					} /*
					 * else { sendInsertRequest(filename, value,
					 * activePorts.get(i) .getPortNumber(), 3); }
					 */
				}
			}

		}

		return null;
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
		insertCarryOver = false;
		TelephonyManager tel = (TelephonyManager) this.getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);

		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		activePorts = new ArrayList<PortHashObject>();
		setSuccessorAndPredecessor();

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
						// Create ContentValues object with the key and value
						// and then call insert() of this node
						insertCarryOver = true;
						currentInsertCount = Integer.parseInt(message
								.getInsertCount());
						if (currentInsertCount > 0) {
							ContentValues mContentValues = new ContentValues();
							mContentValues.put(KEY_FIELD, message.getKey());
							mContentValues.put(VALUE_FIELD, message.getValue());
							Uri mUri = buildUri("content",
									"edu.buffalo.cse.cse486586.simpledynamo.provider");
							ContentResolver mContentResolver = getContext()
									.getContentResolver();
							mContentResolver.insert(mUri, mContentValues);
						}

					}
				}
			}
		}

		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}
	}

	private void setSuccessorAndPredecessor() {
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
//				Log.d(TAG, activePorts.get(i).getPortNumber() + ":@@@"
//						+ activePorts.get(i).getHashedPortNumber());
				if (activePorts.get(i).getPortNumber().equals(portStr)
						&& (i == (activePorts.size() - 1))) {
					successor = activePorts.get(0).getPortNumber();
				} else if (activePorts.get(i).getPortNumber().equals(portStr)) {
					successor = activePorts.get(i + 1).getPortNumber();
				}
			}
//			Log.d(TAG, "@@@Successor:" + successor);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private void sendInsertRequest(String... msgs) {
		Socket socket = null;
		String destination = null;
		ObjectOutputStream objectOutputStream;
		Message message = new Message();
		message.setSenderPort(msgs[0]);

		// Setting the type of request
		message.setMessageType(MessageType.valueOf(msgs[1]));

		if (message.getMessageType().equals(MessageType.INSERT)) {
			message.setKey(msgs[2]);
			message.setValue(msgs[3]);
			destination = msgs[4];
			message.setInsertCount(msgs[5]);
			Log.d(TAG, "@@@Insert request sending to the " + destination);
		}/*
		 * else if ((message.getMessageType().equals(MessageType.QUERY_REQUEST))
		 * || (message.getMessageType().equals(MessageType.DELETE_REQUEST))) {
		 * message.setSelection(msgs[2]); message.setResponsePort(msgs[3]); }
		 */

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
