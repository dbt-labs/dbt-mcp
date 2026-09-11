Submits collected onboarding data to the dbt platform. Safe to call multiple times — each call updates the onboarding record with the data provided.

Call this incrementally as the user provides each piece of information (project name, warehouse credentials, repository, etc.). You do not need to have all data before calling; partial submissions are supported.

Returns the updated onboarding record showing which resources have been configured so far.
