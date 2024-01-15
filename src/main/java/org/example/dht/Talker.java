package org.example.dht;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.example.trajstore.FilterOptions;
import org.example.trajstore.TrajPoint;

/**
 * Talker thread that processes request accepted by listener and writes
 * response to socket.
 * @author Chuan Xia
 *
 */

public class Talker implements Runnable{

	Socket talkSocket;
	private Node local;

	public Talker(Socket _talkSocket, Node _local)
	{
		talkSocket = _talkSocket;
		local = _local;
	}

	public void run()
	{
		InputStream input = null;
		OutputStream output = null;
		try {
			input = talkSocket.getInputStream();
			Request request = Helper.inputStreamToRequest(input);
			String response = processRequest(request);
			if (response != null) {
//				output = talkSocket.getOutputStream();
//				output.write(response.getBytes());
				ObjectOutputStream outputStream = new ObjectOutputStream(talkSocket.getOutputStream());
				outputStream.writeObject(new Request("Resp", response, Request.DataType.Response));
			}
			input.close();
		} catch (IOException e) {
			throw new RuntimeException(
					"Cannot talk.\nServer port: "+local.getAddress().getPort()+"; Talker port: "+talkSocket.getPort(), e);
		}
	}

	private String processRequest(Request request)
	{
		InetSocketAddress result = null;
		String ret = null;
		if (request  == null) {
			return null;
		}
		switch (request.getHeader()){
			case "CLOSEST":{
				assert request.getDataType() == Request.DataType.ID;
				long id = (Long) request.getData();
				result = local.closest_preceding_finger(id);
				String ip = result.getAddress().toString();
				int port = result.getPort();
				ret = "MYCLOSEST_"+ip+":"+port;
				break;
			}
			case "YOURSUCC":{
				result =local.getSuccessor();
				if (result != null) {
					String ip = result.getAddress().toString();
					int port = result.getPort();
					ret = "MYSUCC_"+ip+":"+port;
				}
				else {
					ret = "NOTHING";
				}
				break;
			}
			case "YOURPRE": {
				result = local.getPredecessor();
				if (result != null) {
					String ip = result.getAddress().toString();
					int port = result.getPort();
					ret = "MYPRE_" + ip + ":" + port;
				} else {
					ret = "NOTHING";
				}
				break;
			}
			case "FINDSUCC":{
				assert request.getDataType() == Request.DataType.ID;
				long id = (Long) request.getData();
				result = local.find_successor(id);
				String ip = result.getAddress().toString();
				int port = result.getPort();
				ret = "FOUNDSUCC_"+ip+":"+port;
				break;
			}
			case "IAMPRE":{
				assert request.getDataType() == Request.DataType.Address;
				String data = (String) request.getData();
				InetSocketAddress new_pre = Helper.createSocketAddress(data);
				local.notified(new_pre);
				ret = "NOTIFIED";
				break;
			}
			case "STORE":{
				assert request.getDataType() == Request.DataType.Point;
				TrajPoint point = (TrajPoint) request.getData();
				if(local.store(point)){
					ret = "SUCCESS";
				}
				else ret = "FAIL";
				break;
			}
			case "FETCH": {
				assert request.getDataType() == Request.DataType.Filter;
				FilterOptions filter = (FilterOptions) request.getData();
				ret = local.fetch(filter);
				break;
			}
			case "KEEP":{
				ret = "ALIVE";
				break;
			}
		}
		return ret;
	}
}
