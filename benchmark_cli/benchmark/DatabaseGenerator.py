class DatabaseGenerator():
    """
    todo: implement
    """

    def generate_db(self):
        """Generate database, create tables, indexes, loading data"""
        return NotImplemented

    def reset_db(self):
        """Drop data, reset config, loading data"""
        return NotImplemented

    def generate_refresh_data(self):
        """Generate refresh data: updates and deletes"""
        return NotImplemented

    def generate_queries(self):
        """Create queries and directory for each set"""
        return NotImplemented
