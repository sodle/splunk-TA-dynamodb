import splunklib.client


class CredentialStore:
    """
    Retrieves credentials from the Splunk encrypted credential store.

    :param session_key: Splunk session key
    :type session_key: str
    :param realm: Splunk "realm" to search for credentials
    :type realm: str
    :ivar splunk: Splunk connection
    :type splunk: splunklib.service
    :ivar realm: Splunk "realm" to search for credentials
    :type realm: str
    """

    def __init__(self, session_key, realm):
        self.splunk = splunklib.client.connect(session_key=session_key)
        self.realm = realm

    def get_credential(self, username):
        """
        Retrieves a password from the credential store by the username.

        :param username: The username of the stored credential
        :type username: str
        :return:
        """
        storage_passwords = self.splunk.storage_passwords
        for credential in storage_passwords:
            content = credential.content
            if content.get('realm') == self.realm and content.get('username') == username:
                return content.get('clear_password')
        else:
            raise FileNotFoundError('Could not find the specified credential')


