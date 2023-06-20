## Overview

**Custom Code** allows users to write Java code with data processing logic , upload Java jar packages to [CloudCanal](https://www.clougence.com/), and CloudCanal automatically call these codes during **Full Data** and **Incremental** to achieve various data transformation processing purposes.

## Project Description

- **wide-table** 
  - The fact table and dimension tables join processing code.
- **data-transform** 
  - General data transformation code, e.g., doing operation changes, adding additional fields, filter data,and so on.
- **data-gather** 
  - Aggregate sharding data code.e.g.,eliminate constraint conflicts, adding additional fields and so on.
- **data-compare** 
  - Verification and correction with business logic.
- **business-alert** 
  - Provides corresponding alarms based on stream data.

## Steps
- [Install CloudCanal](https://www.clougence.com/cc-doc/productOP/systemDeploy/install_linux_macos)
- [Create an Custom Code DataJob](https://www.clougence.com/cc-doc/operation/job_manage/create_job/create_process_job)
- DataJob running.

## References
- [Debug Custom Code](https://www.clougence.com/cc-doc/operation/job_manage/convenience_features/debug_customer_code)
- [Log in Custom Code](https://www.clougence.com/cc-doc/operation/job_manage/convenience_features/log_in_customer_code)

## README.md
[English](README.md)
[简体中文](readme/README.zh_CN.md)
