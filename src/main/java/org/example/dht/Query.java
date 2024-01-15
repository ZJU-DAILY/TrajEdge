package org.example.dht;

import java.net.InetSocketAddress;
import java.util.Scanner;
import org.example.trajstore.FilterOptions;
import org.example.trajstore.TrajPoint;

/**
 * Query class that offers the interface by which users can do
 * search by querying a valid chord node.
 * @author Chuan Xia
 *
 */

public class Query {

	private static InetSocketAddress localAddress;
	private static Helper helper;

	public static void main (String[] args) {

		helper = new Helper();

		// valid args
		if (args.length == 2) {

			// try to parse socket address from args, if fail, exit
			localAddress = Helper.createSocketAddress(args[0]+":"+args[1]);
			if (localAddress == null) {
				System.out.println("Cannot find address you are trying to contact. Now exit.");
				System.exit(0);;
			}

			// successfully constructed socket address of the node we are
			// trying to contact, check if it's alive
			String response = Helper.sendRequest(localAddress, new Request("KEEP", null , Request.DataType.None));

			// if it's dead, exit
			if (response == null || !response.equals("ALIVE"))  {
				System.out.println("\nCannot find node you are trying to contact. Now exit.\n");
				System.exit(0);
			}

			// it's alive, print connection info
			System.out.println("Connection to node "+localAddress.getAddress().toString()+", port "+localAddress.getPort()+", position "+Helper.hexIdAndPosition(localAddress)+".");

			// check if system is stable
			boolean pred = false;
			boolean succ = false;
			InetSocketAddress pred_addr = Helper.requestAddress(localAddress, new Request("YOURPRE", null, Request.DataType.None));
			InetSocketAddress succ_addr = Helper.requestAddress(localAddress, new Request("YOURSUCC", null, Request.DataType.None));
			if (pred_addr == null || succ_addr == null) {
				System.out.println("The node your are contacting is disconnected. Now exit.");
				System.exit(0);
			}
			if (pred_addr.equals(localAddress))
				pred = true;
			if (succ_addr.equals(localAddress))
				succ = true;

			// we suppose the system is stable if (1) this node has both valid
			// predecessor and successor or (2) none of them
			while (pred^succ) {
				System.out.println("Waiting for the system to be stable...");
				pred_addr = Helper.requestAddress(localAddress, new Request("YOURPRE", null, Request.DataType.None));
				succ_addr = Helper.requestAddress(localAddress, new Request("YOURSUCC", null, Request.DataType.None));
				if (pred_addr == null || succ_addr == null) {
					System.out.println("The node your are contacting is disconnected. Now exit.");
					System.exit(0);
				}
				if (pred_addr.equals(localAddress))
					pred = true;
				else
					pred = false;
				if (succ_addr.equals(localAddress))
					succ = true;
				else
					succ = false;
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}

			}

			// begin to take user input
			Scanner userinput = new Scanner(System.in);
			while(true) {
				System.out.println("\nPlease enter your search key (or type \"quit\" to leave): ");
				String command = null;
				command = userinput.nextLine();

				// quit
				if (command.startsWith("quit")) {
					System.exit(0);
				}

				// search
				else if (command.length() > 0){
					String cmd = command.split(" ")[0];
					String trajId = command.split(" ")[1];
					long hash = Helper.hashString(trajId);
					System.out.println("\nHash value is "+Long.toHexString(hash));
					InetSocketAddress result = Helper.requestAddress(localAddress, new Request("FINDSUCC", hash, Request.DataType.ID));

					// if fail to send request, local node is disconnected, exit
					if (result == null) {
						System.out.println("The node your are contacting is disconnected. Now exit.");
						System.exit(0);
					}

					// print out response
					System.out.println("\nResponse from node "+localAddress.getAddress().toString()+", port "+localAddress.getPort()+", position "+Helper.hexIdAndPosition(localAddress)+":");
					System.out.println("Node "+result.getAddress().toString()+", port "+result.getPort()+", position "+Helper.hexIdAndPosition(result ));

					// Store value
					if(cmd.startsWith("store")){
						String timestamp = command.split(" ")[2];
						String edgeId = command.split(" ")[3];
						String distance = command.split(" ")[4];
						TrajPoint point = new TrajPoint(Integer.parseInt(trajId), Long.parseLong(timestamp),
							Long.parseLong(edgeId), Double.parseDouble(distance));

						InetSocketAddress dataHostAddress = Helper.createSocketAddress(result.getAddress().toString().substring(1)+":"+result.getPort());
						response = Helper.sendRequest(dataHostAddress, new Request("STORE", point, Request.DataType.Point));
						if(response != null){
							System.out.println(response);
						}
					}
					else if(cmd.startsWith("fetch")){
						FilterOptions filter = new FilterOptions();
						filter.setTrajectoryId(Integer.parseInt(trajId));
						InetSocketAddress dataHostAddress = Helper.createSocketAddress(result.getAddress().toString().substring(1)+":"+result.getPort());
						response = Helper.sendRequest(dataHostAddress, new Request("FETCH",filter , Request.DataType.Filter));
						if(response != null){
							System.out.println(response);
						}
					}
				}
			}
		}
		else {
			System.out.println("\nInvalid input. Now exit.\n");
		}
	}
}
