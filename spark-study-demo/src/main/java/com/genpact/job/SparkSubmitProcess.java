package com.genpact.job;

import java.io.IOException;

import com.github.ywilkof.sparkrestclient.ClusterMode;
import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.JobStatusResponse;
import com.github.ywilkof.sparkrestclient.SparkRestClient;

/**
 */
public class SparkSubmitProcess {
	public static void main(String[] args) throws IOException, FailedSparkRequestException {
		 SparkRestClient sparkRestClient = SparkRestClient.builder()
						.masterHost("58.2.221.224")
						.sparkVersion("1.6.3")
						.masterPort(6066) //submit to spark rest api (6066 from spark webaddress 58.2.221.224:8080)
						.clusterMode(ClusterMode.spark)
						.build();
		 
		 String submissionId = sparkRestClient.prepareJobSubmit()
						.appName("MySparkJob!")
						.mainClass("com.genpact.job.Job")
						.appResource("/bigdata/spark/xsc/stockcalc.jar")
						.submit();
		 
		 JobStatusResponse jobStatus =  sparkRestClient.checkJobStatus()
				    								   .withSubmissionIdFullResponse(submissionId);
				    
		 System.out.println(" success : " +jobStatus.getSuccess());
		 System.out.println(" SubmissionId : " +jobStatus.getSubmissionId());
		 System.out.println(" WorkerHostPort : " +jobStatus.getWorkerHostPort().get());
		 System.out.println(" WorkerId : " +jobStatus.getWorkerId().get());
		 System.out.println(" DriverState : " +jobStatus.getDriverState().toString());
	}
}
