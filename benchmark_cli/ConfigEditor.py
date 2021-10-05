import yaml

from benchmark_cli.constants import CONFIG_FILE_PATH, MAX_REFRESH_FILE_INDEX
from benchmark_cli.utils import create_logger


class ConfigEditor:
    """
    A class provides methods for modifying and store config.yml file.
    """

    _config_file_path: str
    _refresh_file_index: int
    _stream_count: int
    _start_seed: int

    def __init__(self, config_file_path: str = CONFIG_FILE_PATH):
        self._log = create_logger('config_editor')
        self._config_file_path = config_file_path
        self.__load_config()

    def _save_config(self):
        try:
            with open(self._config_file_path, "w") as config_file:
                self._config['refresh_file_index'] = self._refresh_file_index
                self._config['stream_count'] = self._stream_count
                self._config['start_seed'] = self._start_seed
                yaml.dump(self._config, config_file, default_flow_style=False)
            config_file.close()
        except FileNotFoundError as e:
            self._log.error("FileNotFoundError: ", e)
            raise

    def __load_config(self):
        try:
            with open(self._config_file_path, "r") as config_file:
                self._config = yaml.safe_load(config_file)
                self.__validate_config()
            config_file.close()
        except FileNotFoundError as e:
            self._log.error("FileNotFoundError: ", e)
            raise

    def __validate_config(self):
        try:
            self._refresh_file_index = self._config['refresh_file_index']
            self._stream_count = self._config['stream_count']
            self._start_seed = self._config['start_seed']
        except KeyError as e:
            self._log.error("Missing attribute in config file: ", e)
            raise

        if self._refresh_file_index < 1:
            self._log.error(f'Invalid refresh_file_index value: must be between 1 and {MAX_REFRESH_FILE_INDEX}.')
            raise

        if self._stream_count < 1:
            self._log.error(f'Invalid stream_count value: cannot be less than 1.')
            raise
