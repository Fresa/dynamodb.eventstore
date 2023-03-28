using DynamoDB.EventStore.IntegrationTests.Microsoft.System.Net.Http;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.STS;

internal sealed class StsService : ObservableHttpClientHandler
{
    internal const string DnsLabel = "sts";

    protected override string MapRequestToHandlerId(HttpRequestMessage message) => string.Empty;

    internal void RespondWithDefaultAssumeRoleWithWebIdentityResponse()
    {
        On("", (_, _) => Task.FromResult(
            new HttpResponseMessage
            {
                Headers = { { "x-amzn-RequestId", "test-request" } },
                Content = new StringContent($"""
                <?xml version="1.0" encoding="UTF-8"?>
                <AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                <AssumeRoleWithWebIdentityResult>
                   <AssumedRoleUser>
                      <Arn/>
                      <AssumeRoleId/>
                   </AssumedRoleUser>
                   <Credentials>
                      <AccessKeyId>Y4RJU1RNFGK48LGO9I2S</AccessKeyId>
                      <SecretAccessKey>sYLRKS1Z7hSjluf6gEbb9066hnx315wHTiACPAjg</SecretAccessKey>
                      <Expiration>{DateTime.Now.AddHours(1):O}</Expiration>
                      <SessionToken>eyJhbGciOiJIUz</SessionToken>
                   </Credentials>
                </AssumeRoleWithWebIdentityResult>
                <ResponseMetadata/>
                </AssumeRoleWithWebIdentityResponse>
                """)
            }));
    }
}