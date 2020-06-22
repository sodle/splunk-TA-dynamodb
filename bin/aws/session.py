import boto3


def assume_role(session, role_arn, role_session_name, external_id=None):
    """
    Starts an AWS session by assuming a role. Optionally specify an external ID. MFA is not supported.

    :param session: Boto3 session to use as "source" profile
    :type session: boto3.Session
    :param role_arn: Amazon Resource Name (ARN) of role to assume
    :type role_arn: str
    :param role_session_name: Session name for assuming role
    :type role_session_name: str
    :param external_id: External ID for assuming role (optional)
    :type external_id: str, optional
    :return: AWS session
    :rtype: boto3.Session
    """
    sts = session.client('sts')

    assume_role_request = {
        'RoleArn': role_arn,
        'RoleSessionName': role_session_name
    }
    if external_id is not None:
        assume_role_request['ExternalId'] = external_id

    role = sts.assume_role(**assume_role_request)
    creds = role['Credentials']
    return boto3.Session(aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'],
                         aws_session_token=creds['SessionToken'])


def get_static_credentials(profile_name=None, aws_access_key_id=None, aws_secret_access_key=None):
    """
    Starts an AWS session from the given credentials.

    Specify aws_access_key_id and aws_secret_access_key to use the given credentials directly.

    Specify profile_name to use the given AWS CLI profile.

    Specify no arguments to have boto3 autodetect credentials, using the following order of precedence:
    1.  AWS_ environment variables
    2.  [default] AWS CLI profile
    3.  EC2/ECS instance metadata service

    :param profile_name: Name of AWS CLI profile to use
    :type profile_name: str, optional
    :param aws_access_key_id: AWS Access Key ID
    :type aws_access_key_id: str, optional
    :param aws_secret_access_key: AWS Secret Access Key
    :type aws_secret_access_key: str, optional
    :return: AWS session
    :rtype: boto3.Session
    """
    return boto3.Session(profile_name=profile_name, aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)
