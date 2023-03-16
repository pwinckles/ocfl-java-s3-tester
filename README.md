# ocfl-java-s3-tester

This utility is intended to test if ocfl-java works with the new S3 transfer manager when writing to a non-AWS S3
repository.

## Usage

1. Ensure you have a profile in `~/.aws/credentials` that contains the credentials you'd like to use for the test.
2. Run the utility supplying the profile name, region, endpoint url, bucket, and prefix when prompted. Of these,
only the bucket name is strictly required.
3. The test will run and either print "Test passed" or "Test failed" at the end.
4. The test **will not** delete the ocfl repository it creates in your bucket. You must do that manually when you are
done testing. You do not need to delete it between test runs, however.
5. If the test fails, please send me a copy of the log files (`ocfl-java-s3-tester.log` and `aws-sdk.log`).

Example:

```shell
java -jar ocfl-java-s3-tester.jar
```