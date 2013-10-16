/*
 * Copyright 2010 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Modified by Sambit Sahu
 * Modified by Kyung-Hwa Kim (kk2515@columbia.edu)
 * Modified by Jiatian Li and Xiaogang Wang
 * 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AllocateAddressResult;
import com.amazonaws.services.ec2.model.AssociateAddressRequest;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.DeleteSnapshotRequest;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DetachVolumeRequest;
import com.amazonaws.services.ec2.model.DisassociateAddressRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class AwsConsoleApp {

	/*
	 * Important: Be sure to fill in your AWS access credentials in the
	 * AwsCredentials.properties file before you try to run this sample.
	 * http://aws.amazon.com/security-credentials
	 */
	
	static final int sleeptime = 1000 * 60;
	static final int hour = sleeptime * 60 * 24;
	static final int machineAmount = 2;
	
	static AmazonEC2 ec2;
	static AmazonS3Client s3;
	static AWSCredentials credentials;
	static String initialAMI = "ami-e8084981";
	static ArrayList<String> AMIIds;
	static ArrayList<String> instanceIds;
	static ArrayList<String> volumeIds;
	static ArrayList<String> elasticIps;
	
	public static void createInstanceFromAMI(int i) {
		System.out.println("Create instance from AMI " + AMIIds.get(i));
		
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
		
		String keyName = "machine" + (i + 1) + "KeyPair";
		runInstancesRequest.withImageId(AMIIds.get(i))
				.withInstanceType("t1.micro").withMinCount(1).withMaxCount(1)
				.withKeyName(keyName).withSecurityGroups("assignment1")
				.withPlacement(new Placement("us-east-1d"));
		RunInstancesResult result = ec2.runInstances(runInstancesRequest);
		
		//get instanceId from the result
        List<Instance> resultInstance = result.getReservation().getInstances();
        String createdInstanceId = null;
        for (Instance ins : resultInstance){
        	createdInstanceId = ins.getInstanceId();
        	System.out.println("New instance has been created: "+ ins.getInstanceId());
        }
        instanceIds.set(i, createdInstanceId);
	}
	
	public static void createS3Bucket(String bucketName) throws Exception {
		// create bucket
		//bucketName = "columbia.jiatian";
		System.out.println("Creating S3 bucket: " + bucketName);
		s3.createBucket(bucketName);
	}
	
	public static void putObjectInBucket(String key, String content, String filename, String bucketName) throws IOException {
		// set key
		//String key = "object-name.txt";

		// set value
		//File file = File.createTempFile("temp", ".txt");
		File file = File.createTempFile(filename, ".txt");
		file.deleteOnExit();
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		//writer.write("This is a sample sentence.\r\nYes!");
		writer.write(content);
		writer.close();

		// put object - bucket, key, value(file)
		s3.putObject(new PutObjectRequest(bucketName, key, file));
	}
	
	public static String getObjectFromBucket(String bucketName, String key) throws IOException {
		// get object
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				object.getObjectContent()));
		String data = "";
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;
			data += line;
			//System.out.println(data);
		}
		return data;
	}
	
	public static void setupMachines() throws Exception {
		try {

			/*********************************************
			 * #2.1 Create a volume
			 *********************************************/
			// create a volume
			// CreateVolumeRequest cvr = new CreateVolumeRequest();
			// cvr.setAvailabilityZone("us-east-1d");
			// cvr.setSize(1); //size = 10 gigabytes
			// CreateVolumeResult volumeResult = ec2.createVolume(cvr);
			//
			// String createdVolumeId = volumeResult.getVolume().getVolumeId();
			// System.out.println("VolumeId: " + createdVolumeId);
			// while (true) {
			// ArrayList<String> volumeIds = new ArrayList<String>();
			// volumeIds.add(createdVolumeId);
			// DescribeVolumesRequest dvr = new
			// DescribeVolumesRequest(volumeIds);
			// DescribeVolumesResult result = ec2.describeVolumes(dvr);
			// if (result.getVolumes().get(0).getState().equals("available"))
			// break;
			// System.out.println("Volume state: " +
			// result.getVolumes().get(0).getState());
			// Thread.sleep(1000);
			// }
			
			System.out.println("Setting up all machines...");
			for (int i = 0; i < AMIIds.size(); i++)
				createInstanceFromAMI(i);
			
			Thread.sleep(sleeptime); // wait for 60 sec
			/*
			 * start 2 instances
			 */
			for (int i = 0; i < instanceIds.size(); i++)
				startInstance(instanceIds.get(i));
			
			Thread.sleep(sleeptime); // wait for 60 sec
			
			/*********************************************
			 * #2.2 Attach the volume to the instance respectively
			 *********************************************/
			for (int i = 0; i < machineAmount; i++) {
				AttachVolumeRequest avr = new AttachVolumeRequest();
				avr.setVolumeId(volumeIds.get(i));
				avr.setInstanceId(instanceIds.get(i));
				avr.setDevice("/dev/sdf");
				ec2.attachVolume(avr);
			}
			Thread.sleep(sleeptime); // wait for 60 sec
			
			/*
			 * associate ip address
			 */
			for (int i = 0; i < instanceIds.size(); i++)
				associateAddressWithInstance(i);
			Thread.sleep(sleeptime); // wait for 60 sec
			
			

		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
	}
	
	public static void allocateElasticIP() {
		//allocate
		for (int i = 0; i < machineAmount; i++) {
			AllocateAddressResult elasticResult = ec2.allocateAddress();
			elasticIps.add(elasticResult.getPublicIp());
			System.out.println("New elastic IP: " + elasticIps.get(i));
		}
	}
	
	/*
	 * associate elastic ip address i with instance i
	 */
	public static void associateAddressWithInstance(int i) {
		//associate
		AssociateAddressRequest aar = new AssociateAddressRequest();
		aar.setInstanceId(instanceIds.get(i));
		aar.setPublicIp(elasticIps.get(i));
		ec2.associateAddress(aar);
		System.out.println("Associated address " + elasticIps.get(i) + " with instance " + instanceIds.get(i));
	}
	
	/*
	 * disassociate Address i With Instance i
	 */
	public static void disassociateAddressWithInstance(int i) {
		//disassociate
		DisassociateAddressRequest dar = new DisassociateAddressRequest();
		dar.setPublicIp(elasticIps.get(i));
		ec2.disassociateAddress(dar);
		System.out.println("Diassociated address " + elasticIps.get(i) + " with instance " + instanceIds.get(i));
	}
	
	public static void test() throws Exception {
		createInstanceFromAMI(0);
		Thread.sleep(60000); // wait for 60 sec for creating
		startInstance(instanceIds.get(0));
		Thread.sleep(50000); // wait for 30 sec for starting 
		attachVolume(instanceIds.get(0), volumeIds.get(0));
		Thread.sleep(60000); // wait for 60 sec
		associateAddressWithInstance(0);
		Thread.sleep(60000); // wait for 60 sec
		disassociateAddressWithInstance(0);
		Thread.sleep(60000); // wait for 60 sec
		detachVolume(instanceIds.get(0), volumeIds.get(0));
		Thread.sleep(30000); // wait for 30 sec for detaching
		stopInstance(instanceIds.get(0));
		createAMIfromInstance(0);
		Thread.sleep(30000);
		removeImage(AMIIds.get(0));
		terminateInstance(instanceIds.get(0));
	}
	
	public static void createShareFileInS3(String bucketName, String filename) throws Exception {
		createS3Bucket(bucketName);
		String content = "This is a shared file just to test S3 storage!";
		putObjectInBucket(filename + ".txt", content, filename, bucketName);
	}
	
	public static void testS3(String bucketName) throws IOException {
		String data = getObjectFromBucket(bucketName, "shared.txt");
		System.out.println("The following content is from the shared file \"shared.txt\" from S3: " + data);
	}
	
	public static void main(String[] args) throws Exception {

		credentials = new PropertiesCredentials(
				AwsConsoleApp.class
						.getResourceAsStream("AwsCredentials.properties"));

		/*********************************************
		 * #1 Create Amazon Client object
		 **********************************************/
		ec2 = new AmazonEC2Client(credentials);
		s3 = new AmazonS3Client(credentials);
		
		elasticIps = new ArrayList<String>(2);
		AMIIds = new ArrayList<String>(2);
		AMIIds.add(initialAMI);
		AMIIds.add(initialAMI);
		//ArrayList<String> instanceIds = createInstancesFromAMIs();
		// hardcode instance ids for now
		instanceIds = new ArrayList<String>(2); 
		instanceIds.add("i-7e2ddf18");
		instanceIds.add("i-2e892e49");
		// We assume that we've already created an instance. Use the id of the
		// instance.
		// String instanceId = "i-9dac58e4"; //put your own instance id to test
		// this code.

		// volume ids
		volumeIds = new ArrayList<String>(2);
		volumeIds.add("vol-4e2c800d");
		volumeIds.add("vol-9153ffd2");
		allocateElasticIP();
		
		String bucketName = "jiatian.cloud.assignment1.share";
		createShareFileInS3(bucketName, "shared");
		testS3(bucketName);
//		String state = getInstanceState("i-814b31f8");
//		System.out.println("state: " + state);
		//test();
		//testCPUutlilization();
		int day = 1;
		while (true) {
			System.out.println("Day " + day + " starts");
			setupMachines();	
			while (true) {
				int terminatedCount = terminateIdleMachines();
				if (terminatedCount == machineAmount || !isWorkingHour())
					break;
				Thread.sleep(sleeptime);
				System.out.println("System functioning...");
			}
			System.out.println("Day " + day + " is over");
			day++;
			if (day > 10)   // run the system for 10 days
				break;
			terminateAllMachines();
			while (!isWorkingHour())
				Thread.sleep(hour);
		}
		
		
		/*********************************************
		 * #4 shutdown client object
		 *********************************************/
		ec2.shutdown();
		s3.shutdown();
	}
	
	public static void testCPUutlilization() throws Exception {
		String instanceId = "i-2e892e49";
		while (true) {
			Thread.sleep(5000);
			if (isIdle(instanceId)) {
				stopInstance(instanceId);
				break;
			}
		}
	}
	
	public static boolean isWorkingHour() {
		Date date = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("HH");
		int hour = Integer.parseInt(ft.format(date));
		//if (hour >= 8 && hour <= 17)
		if (hour >= 0 && hour <= 24) //all day is working hour just for testing
			return true;		
		else
			return false;
	}
	
	public static void terminateAllMachines() throws Exception {
		System.out.println("Terminating all machines...");
	}
	
	public static String getInstanceState(String instanceId) {
		DescribeInstancesRequest dir =new DescribeInstancesRequest();
		List<String> instanceIds = new ArrayList<String>();
		instanceIds.add(instanceId);
		dir.setInstanceIds(instanceIds);
        DescribeInstancesResult result = ec2.describeInstances(dir);
        List<Reservation> list  = result.getReservations();
        //String instanceName = list.get(0).getInstances().get(0).getTags().get(0).getValue();
        String instanceState = list.get(0).getInstances().get(0).getState().getName();
        //System.out.println(instanceName + " state: " + instanceState);
        return instanceState;
//        System.out.println("-------------- status of instances -------------");
//        for (Reservation res:list){
//             List <Instance> instancelist = res.getInstances();
//
//             for (Instance instance:instancelist){
//
//                 System.out.println("Instance Status : "+ instance.getState().getName());
//                 List <Tag> t1 =instance.getTags();
//                 for (Tag teg:t1){
//                     System.out.println("Instance Name   : "+teg.getValue());
//                 }
//
//             }     
//        System.out.println("------------------------------------------------");
//          }
	}
	
	public static int terminateIdleMachines() throws Exception {
		System.out.println("Terminating idle machines...");
		int terminatedCount = 0;
		for (int i = 0; i < instanceIds.size(); i++) {
			System.out.println("checking instance " + instanceIds.get(i) + "'s state");
			String instanceState = getInstanceState(instanceIds.get(i));
			System.out.println("instance " + instanceIds.get(i) + "'s state: " + instanceState);
			if (instanceState.equals("terminated")) {
				terminatedCount++;
			} else { // is not terminated
				//isIdle
				if (isIdle(instanceIds.get(i))) {
					shutdownInstance(i);
					terminatedCount++;
				}
			}
		}
		return terminatedCount;
	}
	
	public static void shutdownInstance(int i) throws Exception {
		disassociateAddressWithInstance(i);
		Thread.sleep(60000); // wait for 60 sec
		detachVolume(instanceIds.get(i), volumeIds.get(i));
		Thread.sleep(sleeptime); // wait for 60 sec
		stopInstance(instanceIds.get(i));
		Thread.sleep(sleeptime); // wait for 60 sec
		removeImage(AMIIds.get(i));
		Thread.sleep(sleeptime); // wait for 60 sec
		createAMIfromInstance(i);
		Thread.sleep(sleeptime); // wait for 60 sec
		terminateMachine(instanceIds.get(i));
		Thread.sleep(sleeptime); // wait for 60 sec
	}
	
	public static void startInstance(String instanceId) {
//		if (!getInstanceState(instanceId).equals("stopped"))
//			return;
		System.out.println("Starting the Instance " + instanceId);
		List<String> instanceIds = new ArrayList<String>();
		instanceIds.add(instanceId);
		StartInstancesRequest startIR = new StartInstancesRequest(instanceIds);
		ec2.startInstances(startIR);
	}
	
	public static void terminateInstance(String instanceId) {
		System.out.println("Terminate the Instance");
		List<String> instanceIds = new ArrayList<String>();
		instanceIds.add(instanceId);
        TerminateInstancesRequest tir = new TerminateInstancesRequest(instanceIds);
        ec2.terminateInstances(tir);
	}
	
	public static void stopInstance(String instanceId) {
		if (!getInstanceState(instanceId).equals("running"))
			return;
		System.out.println("Stopping the Instance " + instanceId);
        List<String> instanceIds = new LinkedList<String>();
        instanceIds.add(instanceId);
        
        //stop
        StopInstancesRequest stopIR = new StopInstancesRequest(instanceIds);
        ec2.stopInstances(stopIR);
	}
	
	public static void attachVolume(String instanceId, String volumeId) {
		AttachVolumeRequest avr = new AttachVolumeRequest();
		avr.setVolumeId(volumeId);
		avr.setInstanceId(instanceId);
		avr.setDevice("/dev/sdf");
		ec2.attachVolume(avr);
		System.out.println("attached volume " + volumeId + " with instance " + instanceId);
	}
	public static void detachVolume(String instanceId, String volumeId) {
		/*********************************************
		 * #2.3 Detach the volume from the instance
		 *********************************************/
		DetachVolumeRequest dvr = new DetachVolumeRequest();
		dvr.setVolumeId(volumeId);
		dvr.setInstanceId(instanceId);
		ec2.detachVolume(dvr);
		System.out.println("detached volume " + volumeId + " with instance " + instanceId);
	}
	
	//create AMI from instanceIds[i]
	public static void createAMIfromInstance(int i) throws Exception {
		System.out.println("create AMI from instance...");
		while (true) { //busy wait until instance is stopped
			String instanceState = getInstanceState(instanceIds.get(i));
			System.out.println("Instance state: " + instanceState);
			if (instanceState.equals("stopped"))
				break;
			System.out.println("Waiting until machine" + i + " is stopped");
			Thread.sleep(50000);
		}
		CreateImageRequest cir = new CreateImageRequest(instanceIds.get(i), "machine" + i);
		CreateImageResult result = ec2.createImage(cir);
		System.out.println("Created new AMI for machine" + i + ": " + result.getImageId());
		AMIIds.set(i, result.getImageId());
	}
	
	/**
	 * Removes an ami and its snapshot.
	 * @param amiID
	 * @param snapshotID
	 * @throws Exception 
	 */
	public static void removeImage(String AMIId) throws Exception {
		if (AMIId.equals(initialAMI))
			return;
		System.out.println("remove AMI: " + AMIId);
		ArrayList<String> owners = new ArrayList<String>();
		owners.add("self");
        DescribeImagesResult result = ec2.describeImages(new DescribeImagesRequest().withImageIds(AMIId).withOwners(owners));
        if (!result.getImages().isEmpty()) {
            ec2.deregisterImage(new DeregisterImageRequest(AMIId));
            Thread.sleep(5000);
            for (BlockDeviceMapping blockingDevice : result.getImages().get(0).getBlockDeviceMappings()) {
                if (blockingDevice.getEbs() != null) {
                    ec2.deleteSnapshot(new DeleteSnapshotRequest().withSnapshotId(blockingDevice.getEbs().getSnapshotId()));
                }
            }
        }
	    System.out.println("Removed " + AMIId + " successfully");
	}
	
	public static boolean isIdle(String instanceId) {
		Double averageCPU = getCPUutlilization(instanceId);
		if (averageCPU != null && averageCPU < 1)  // tentative
			return true;
		else
			return false;
	}
	
	public static Double getCPUutlilization(String instanceId) {
		Double averageCPU = null;
		try{
 				
			/***********************************
			 *   #3 Monitoring (CloudWatch)
			 *********************************/
			
			//create CloudWatch client
			AmazonCloudWatchClient cloudWatch = new AmazonCloudWatchClient(credentials) ;
			
			//create request message
			GetMetricStatisticsRequest statRequest = new GetMetricStatisticsRequest();
			
			//set up request message
			statRequest.setNamespace("AWS/EC2"); //namespace
			statRequest.setPeriod(60); //period of data
			ArrayList<String> stats = new ArrayList<String>();
			
			//Use one of these strings: Average, Maximum, Minimum, SampleCount, Sum 
			stats.add("Average"); 
			stats.add("Sum");
			statRequest.setStatistics(stats);
			
			//Use one of these strings: CPUUtilization, NetworkIn, NetworkOut, DiskReadBytes, DiskWriteBytes, DiskReadOperations  
			statRequest.setMetricName("CPUUtilization"); 
			
			// set time
			GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
			calendar.add(GregorianCalendar.SECOND, -1 * calendar.get(GregorianCalendar.SECOND)); // 1 second ago
			Date endTime = calendar.getTime();
			calendar.add(GregorianCalendar.MINUTE, -10); // 10 minutes ago
			Date startTime = calendar.getTime();
			statRequest.setStartTime(startTime);
			statRequest.setEndTime(endTime);
			
			//specify an instance
			ArrayList<Dimension> dimensions = new ArrayList<Dimension>();
			dimensions.add(new Dimension().withName("InstanceId").withValue(instanceId));
			statRequest.setDimensions(dimensions);
			
			//get statistics
			GetMetricStatisticsResult statResult = cloudWatch.getMetricStatistics(statRequest);
			
			//display
			System.out.println(statResult.toString());
			List<Datapoint> dataList = statResult.getDatapoints();
			Date timeStamp = null;
			for (Datapoint data : dataList){
				averageCPU = data.getAverage();
				timeStamp = data.getTimestamp();
				System.out.println("Average CPU utlilization for last 10 minutes: "+averageCPU);
				System.out.println("Totl CPU utlilization for last 10 minutes: "+data.getSum());
			}
            
		} catch (AmazonServiceException ase) {
		    System.out.println("Caught Exception: " + ase.getMessage());
		    System.out.println("Reponse Status Code: " + ase.getStatusCode());
		    System.out.println("Error Code: " + ase.getErrorCode());
		    System.out.println("Request ID: " + ase.getRequestId());
		}
		return averageCPU;
	}
	
	public static void terminateMachine(String intanceId) {
		ArrayList<String> instanceIds = new ArrayList<String>();
		instanceIds.add(intanceId);
		TerminateInstancesRequest tir = new TerminateInstancesRequest(instanceIds);
        ec2.terminateInstances(tir);
        System.out.println("Instance " + intanceId + " terminated");
	}
}