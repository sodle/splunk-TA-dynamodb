import os
from os import path
import sys
import re

sys.path.insert(0, path.join(path.dirname(__file__), "..", "lib"))


class Checkpointer:
    """
    Manages checkpoint files for a Splunk modular input.

    :param checkpoint_dir: Splunk-provided path for the input's checkpoints
    :type checkpoint_dir: str
    :ivar checkpoint_dir: Splunk-provided path for the input's checkpoints
    :type checkpoint_dir: str
    """

    def __init__(self, checkpoint_dir):
        self.checkpoint_dir = checkpoint_dir

        # Create dir if it's not already there
        if not path.isdir(checkpoint_dir):
            os.mkdir(checkpoint_dir)

    forbidden_characters = re.compile(r'[^A-Za-z0-9_\-.]+')

    def escape_filename(self, filename):
        """
        Removes nasty characters from a filename and replaces them with hyphens.

        Allows characters A-Z, a-z, 0-9, -, _, .

        :param filename: filename to clean
        :type filename: str
        :return: Filename with nasty characters replaced by hyphens
        :rtype: str
        """
        return self.forbidden_characters.sub('-', filename)

    def get_checkpoint_path(self, checkpoint_key):
        """
        Determines the "sanitized" path to a checkpoint file.

        :param checkpoint_key: "Key" name of the checkpoint
        :type checkpoint_key: str
        :return: Path to checkpoint file
        :rtype: str
        """
        clean_name = self.escape_filename(checkpoint_key)
        return path.join(self.checkpoint_dir, clean_name)

    def get_checkpoint(self, checkpoint_key):
        """
        Gets the checkpoint value for the specified key, if a value exists. Returns None otherwise.

        :param checkpoint_key: "Key" name of the checkpoint to retrieve
        :type checkpoint_key: str
        :return: Checkpoint value if it exists, otherwise None
        :rtype: str, None
        """
        checkpoint_path = self.get_checkpoint_path(checkpoint_key)
        if path.isfile(checkpoint_path):
            with open(checkpoint_path, 'r') as checkpoint_file:
                return checkpoint_file.read()
        else:
            return None

    def put_checkpoint(self, checkpoint_key, checkpoint_value):
        """
        Puts a checkpoint value for the specified key. Overwrites any previous value for that key.

        :param checkpoint_key: "Key" name of the checkpoint to write
        :type checkpoint_key: str
        :param checkpoint_value: Value to write to the checkpoint
        :type checkpoint_value: str
        """
        checkpoint_path = self.get_checkpoint_path(checkpoint_key)
        with open(checkpoint_path, 'w') as checkpoint_file:
            checkpoint_file.write(checkpoint_value)
