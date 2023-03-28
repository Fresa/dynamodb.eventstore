using Amazon.Runtime;
using Amazon.Runtime.SharedInterfaces;
using Amazon.SecurityToken;

namespace DynamoDB.EventStore.IntegrationTests.Amazon;

internal sealed class ConfigurableAssumeRoleWithWebIdentityCredentials : AssumeRoleWithWebIdentityCredentials
{
    private readonly HttpClientFactory _clientFactory;

    public ConfigurableAssumeRoleWithWebIdentityCredentials(HttpClientFactory clientFactory) :
        base($"{Directory.GetCurrentDirectory()}/example_aws_identity.token", "test", "test-session")
    {
        _clientFactory = clientFactory;
    }

    protected override ICoreAmazonSTS_WebIdentity CreateClient() =>
        new AmazonSecurityTokenServiceClient(new AnonymousAWSCredentials(), new AmazonSecurityTokenServiceConfig
        {
            HttpClientFactory = _clientFactory
        });
}