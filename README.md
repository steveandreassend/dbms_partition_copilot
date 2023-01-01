# dbms_partition_wrangler
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
* Current employees of Oracle Corporation, or of its subsidiaries, or of its entities where it has a controlling interest, are specifically requested not to contribute any designs or code to this project. This is to prevent Oracle Corporation claiming ownership of designs or intellectual property because its employees contributed to an open source project, potentially in a related area of business, regardless if it is done in their own time, and regardless of whether company property is used.

Why:
* Because there is no consistency in how data controllers and data processors handle bulk data. It is difficult to follow best practices and this project provides an easy way to adopt them.
* Entities may be out of compliance with data privacy legislation such as GDPR. Mass data leaks are occurring regularly and they often concern data that should not have have been retained once it is no longer of use.
* To reduce the energy footprint of storing and processing bulk data by avoiding unnecessary and excessive resource consumption. Data centers are large energy consumers and in many countries they are powered by fossil fuels, responsible for an estimated 3% of greenhouse gas emissions.
* Most databases are not secure. In 2018 it was claimed by one commentator that only 1% of the databases in the world are encrypted.
* Why a wrangler? Because partitions need to be managed with care. Handling must be orchestrated by policy and fully automated to prevent human error.
* Why FOSS? This project is intended as a benefit to a society and so the code can be subject to scrutiny, testing, contributions, and adoption by the community. It is provided here under the MIT license.

How:

This package provides an opinionated design for managing RANGE partitions in a package called DBMS_PARTITION_WRANGLER.
RANGE partitioning is the most common partitioning method employed in the field. It can be combined with sub-partitioning with LIST and HASH partitioning whereby the top-level RANGE partitioning provides the metadata, and the sub-partitionins provide the physical storage. The intention wit this API is to leverage Oracle's built-in capabilities and in some cases improve upon the newer extensions provided.
See the worked examples in Examples.txt to learn how to use DBMS_PARTITION_WRANGLER.

Compatibility:

This package is intended to work with Oracle Database versions 12.1 and higher. Some newer features will be detected and leveraged where necessary. As newer versions of Oracle Database are released with new partitioning features, this framework will need to be revised. Users of this package are expected to review the code, test the functionality with their database release, adapt it as necessary, and assume all responsibility and liability for using it as per the stated terms of the MIT license.

Background Notes:

What is Oracle Partitioning: https://www.oracle.com/docs/tech/database/partitioning-technical-brief-12c.pdf
What is a wrangler: https://en.wikipedia.org/wiki/Wrangler_(profession)
