Get detailed information about one or more exposures by name or unique IDs.

Parameters:
- unique_id (optional): ID of exposure (e.g., ["exposure.project.exposure1", "exposure.project.exposure2"]) (more efficient - uses GraphQL filter)

Returns a list of detailed information dictionaries, each including:
- name: The name of the exposure
- description: Detailed description of the exposure
- exposureType: Type of exposure (application, dashboard, analysis, etc.)
- maturity: Maturity level of the exposure (high, medium, low)
- ownerName: Name of the exposure owner
- ownerEmail: Email of the exposure owner
- url: URL associated with the exposure
- label: Optional label for the exposure
- parents: List of parent models/sources that this exposure depends on
- meta: Additional metadata associated with the exposure
- freshnessStatus: Current freshness status of the exposure
- uniqueId: The unique identifier for this exposure

Example usage:
- Get an exposure by unique ID: get_exposure_details(unique_id="exposure.analytics.customer_dashboard")
