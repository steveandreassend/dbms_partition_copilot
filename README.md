# dbms_partition_copilot
This project offers fully automated range partitioning for Oracle Database that follows best practices for handling data.

Mission Statement: To help custodians of data that concerns the general public to better manage that data.

Goals:
* Facilitate the uptake of best practices for handling bulk data in an Oracle Database.
* Facilitate the purging of that data when it is no longer of relevance and there is no justification for its retention by the data controller.
* Facilitate the reduction of online storage, archive storage, backup storage, compute, and electricity consumption that is incurred by hosting the data set.
* Facilitate performance improvements for the online processing of large data sets by reducing the size of the working set.
* Encourage the use of security controls such as encrpytion and auditing to protect data.

Intended Audience:
* Data processors and data controllers in retail, healthcare, telecommunications, utilities, transportation, hospitality, hotels, and financial services, among others, that more often than not store bulk personal data in an Oracle Database.
* The data in scope concerns for example, your personal details, where you live and work, your personal contact details, your national ID, what you buy, where you buy, how much you spend, your entire medical history, who you communiucate with, the contents of your communications, the apps you use, the websites you visit, your energy consumption, where you dine-out and what you ordered, where you travel, where you stayed and what you consumed, your accounts and entire financial history, ....
* Current employees of Oracle Corporation, or of its subsidiaries, or of its entities where it has a controlling interest, are specifically requested not to contribute any designs or code to this project. This is to prevent Oracle Corporation claiming ownership of designs or intellectual property because its employees contributed to an open source project, potentially in a related area of business, regardless if it is done in their own time, and regardless of whether company property is used or not. You are however welcome to use the package as per the MIT license terms.

Why:
* Because there is no consistency in how data controllers and data processors handle bulk data. It is difficult to follow best practices and this project provides an easy way to adopt them.
* Entities may be out of compliance with data privacy legislation such as GDPR. Mass data leaks are occurring regularly and they often concern data that should not have have been retained once it is no longer of use.
* To reduce the energy footprint of storing and processing bulk data by avoiding unnecessary and excessive resource consumption. Data centers are large energy consumers and in many countries they are powered by fossil fuels, responsible for an estimated 3% of greenhouse gas emissions.
* Most databases are not secure. In 2018 it was claimed by one commentator that only 1% of the databases in the world are encrypted.
* Because partitioning offers the potential for performance gains in the orders of magnitude by shrinking the size of the working data set size.
* Why a wrangler? Because partitions need to be managed with care. Handling must be orchestrated by policy and fully automated to prevent human error.
* Why FOSS? This project is intended as a benefit to a society and so the code can be subject to scrutiny, testing, contributions, and adoption by the community. It is provided here under the MIT license.

How:

This package provides an opinionated design for managing RANGE partitions in a package called DBMS_PARTITION_WRANGLER.
RANGE partitioning is the most common partitioning method employed in the field. It can be combined with sub-partitioning with LIST and HASH partitioning whereby the top-level RANGE partitioning provides the metadata, and the sub-partitionins provide the physical storage. The intention wit this API is to leverage Oracle's built-in capabilities and in some cases improve upon the newer extensions provided.
See the worked examples in Examples.txt to learn how to use DBMS_PARTITION_WRANGLER.

Compatibility:

This package is intended to work with Oracle Database versions 12.1 and higher. Some newer features will be detected and leveraged where necessary. As newer versions of Oracle Database are released with new partitioning features, this framework will need to be revised. Users of this package are expected to review the code, test the functionality with their database release, adapt it as necessary, and assume all responsibility and liability for using it as per the stated terms of the MIT license.

In future, it is hoped that the same functionality will be ported to Postgres and MySQL.

Features:

* Provide the ability to automatically drop partitions based upon a configurable retention period. This simplifies the operation and avoids the need for operator involvement. A partition drop is fast and does not generate redo to purge data.
* Provide the ability to automatically truncate partitions and shrink tablespace storage based upon a configurable retention period. This simplifies the operation and avoids the need for operator involvement. This is an alternative option to dropping partitions.
* Provide the ability to automatically compress partitions when they become inactive based upon a configurable compression setting. This reduces the storage costs by reducing the amount of data being stored. It also improves performance by reducing the amount of disk IO activity needed to read data.
* Provide the ability to automatically make partitions and tablespaces READ-ONLY when they become inactive. This makes it possible to shrink the size of the RMAN backups by excluding READ-ONLY tablespaces.
* Provide the ability to automatically move partitions to a different ASM Disk Group when they become inactive. This lowers storage cost by allowing cold partitions to be stored on cheaper storage hardware.
* Provide the ability to ensure partitions are stored in dedicated or rolled-up tablespaces. This provides the aforementioned manageability benefits.
* Provide the ability to set a policy to store active and inactive partitions in different memory segments. This provides performance benefits by allowing active data to have a different memory entitlement than historical data. For instance active partitions can be configured to use DEFAULT or KEEP buffer caches, while inactive data can be stored in RECYCLE so that it is segregated and does not flush active partitions out of memory.
* Provide the ability to use a custom DB_BLOCK_SIZE. For large transaction records it can be beneficial for performance use a 32KB block size instead of the default of 8KB because it requires 4x fewer disk IOPS to read the same amount of data.
* Overcome the limitations of Internal Range Partitioning; e.g. use a partition naming scheme, use a better physical storage design, pre-allocate storage.
* Enforce the usage of BIGFILE tablespaces to simplify capacity management.
* Provide the ability to specify configure TDE tablespace encryption to comply with security mandates such as GDPR and PCI-DSS.
* Ensure that global indexes are maintained during partition COMPRESS, DROP, MOVE, TRUNCATE operations.
* Provide the option to utilize Internal Range Partitioning, but still make use of the partition archiving functionality.
* Provide the ability to audit all partition maintenance activity.
* Provide multiple user accounts to securely share the same API for managing partitions.
* Send all partition maintenance events to the database alert log for traceability purposes.
* Provide the ability to employ sub-partitioning using HASH or LIST partitioning to provide more granular physical storage.
* Provide observability for managing all partition maintenance jobs through DBMS_SCHEDULER.
* Provide reliability in Data Guard configurations by running all partition maintenance jobs through DBMS_SCHEDULER on the primary database.
* Support ENABLE ROW MOVEMENT (updates to the partition key) by setting different schedules for READ ONLY and COMPRESS operations for inactive partitions.
* Provide diagnostics by checking the partitioning configuration.
* Provide recommendations by checking the partitioning configuration. For instance the optimal DBMS_STATS settings.
* Provide the ability to utilize parallel DDL to speed up operations such as COMPRESS.
* Provides serialization controls to avoid conflicting jobs becoming kamikaze.
* Provides a common framework for partitioning so that partition-wise joins and partition pruning is optimal across tables and schemas and applications.
* Provides an optional integration with Flashback Database to allow emergency rollbacks using Guaranteed Restore Points when dropping partitions.
* Provides control of feature usage to ensure compliance with your Oracle licenses.

Restrictions:

* Mandates the usage of ASM for data storage.
* Mandates the usage of BIGFILE tablespaces.
* Excludes the use of Reference Partitioning. You should consider not using primary keys or foreign keys on range partitioned tables.
* Excludes the use of SPLIT and MERGE partitions. This will confuse the interval calculations.

Background Notes:

* What is Oracle Partitioning: https://www.oracle.com/docs/tech/database/partitioning-technical-brief-12c.pdf
* Get the best out of Oracle Partitioning https://www.oracle.com/docs/tech/partitioning-guide-2703320.pdf

