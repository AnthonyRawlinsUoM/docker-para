from lfmc.process.QueryAnalyzer import QueryAnalyzer
from lfmc.process.ResultFinder import ResultFinder
from lfmc.process.IngestionWorker import IngestionWorker
from lfmc.results.Cache import Cache

class Workflow:
    
    # Staged series of tasks that must be executed in order but each task can be parallel executed.
    
    # Analyze the Query
        # Bounds checking - Spatial
        # Bounds checking - Temporal
        # Intersections with cached responses
    
    # Determine what data might be mising from our indexed data (if any) -PP
        # Gather missing sources -PP
        # Process the sources -PP
        # ingest and index the data -PP
    
    # Assemble the Response
    # Fill the gaps in our cached Data
    # Create the complete response from the cache.
        # Signal that the cache has been updated