import yaml

class Config:
    def __init__(self, path):
        with open(path, 'r') as cfg_file:
            self.config = yaml.safe_load(cfg_file)
    
    def get_config(self, app = "app2"):
        return self.config[app]