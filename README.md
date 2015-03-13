# owb2infa
This project is to create a tool to support automatically migrate mapping from OWB to Informatica.

* Data flow

First, the OWB metadata can be retrieved using OWB Java API and generate the intermedia xml files. Then the tool will read these metadata and manipulate Informatica mapping xml using Design API. Please check the high level architecture diagram: 
<br>
![the high level arch](doc/HighLevelArch.png)
<br>
* Demo video

http://youtu.be/Evujv7n8TiM

* Pre-requirements

 - Informatica Java API
 - JAXB libraries in Informatica 
 - Oracle Warehouse Builder (OWB) Java API
 -  [Xstream](http://xstream.codehaus.org "Xstream")
 -   Oracle JDBC driver
 -   Log4j 2

## License
This software is issued under the Apache 2 license.
