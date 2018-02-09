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
						.masterPort(6066)
						.clusterMode(ClusterMode.yarn)
						.build();
		 
//		 String submissionId = new JobSubmitRequestSpecificationImpl(sparkRestClient).appName("MySparkJob!").appResource("file://bigdata/spark/xsc/stockcalc.jar")
//		 .mainClass("com.genpact.job.Job").submit();
		 
		 String submissionId = sparkRestClient.prepareJobSubmit()
						.appName("MySparkJob!")
						.mainClass("com.genpact.job.Job")
						.appResource("/bigdata/spark/xsc/stockcalc.jar")
						.submit();
		 JobStatusResponse jobStatus =  sparkRestClient.checkJobStatus()
				    								   .withSubmissionIdFullResponse(submissionId);
				    
		 System.out.println(jobStatus.getMessage());
	}
}
