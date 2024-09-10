\c 25 100
//##################################GLOBAL CONFIG#################################//
OPTS:{upper[key x]!value x}.Q.opt .z.x
DEVMODE:`DEV in key OPTS
NOEXIT:`NOEXIT in key OPTS
DBI_DB:`:/Users/michael/q/projects/dbi/db
/List of DBs to catalogue (will be a json config later, from some front end confiug application)
DBS:hsym`$("/Users/michael/q/projects/genome/bin/snpDive/maindb";
           "/Users/michael/q/projects/Memories-master/db";
           "/Users/michael/q/projects/gaeilge/db";
           "/Users/michael/q/projects/genome/bin/snpDive/demodb";
           "/Users/michael/q/projects/genome/bin/snpDive/dbcopy";
           "/Users/michael/q/projects/genome/bin/snpDive/testdb";
           "/Users/michael/q/projects/vcfAnalysis-master/db");

.util.logm:{-1("@"sv string(x;y))," - ",string[.z.T]," - ",z;}[.z.u;.z.h;] /log message
//use unix 'file' command, return filenames!filetypes
.util.fileInfo:{[fpths]
 fpths:(0#`),fpths; /expects list of files
 res:first each system each"file ",/:1_'string fpths; /run unix 'file' command
 :(!). flip{`$@[trim":"vs x;0;{last"/"vs x}]}each res; /parse result and return
 }
//##################################MAIN LOGIC#################################//
catalogueColumn:{[coldata]
 2".";
 vcount:count coldata; /col count
 vtypes:type each coldata; /each col value type
 deftype:first key desc count each group vtypes; /the default type is the most common type
 difftypeidxs:where not deftype=vtypes; /indexes where the value type differs from the default type
 if[deftype~-20h;deftype:-11h;]; /enum syms as syms
 if[deftype within 21 75h;coldata:string coldata;deftype:10h]; /other enums cast to string
 if[deftype>75h;deftype:0h;]; /general just use general list
 nullval:$[deftype<0;first deftype$();deftype$()]; /Create a null value to check. Types less than 0(negative) are atomic (`first`), others aren't
 nullidxs:where nullval~/:coldata; /Return indexes where coldata value matches our default null
 :`colcount`coltype`difftypes`nullvals!(vcount;deftype;difftypeidxs;nullidxs);
 }

catalogueSplayed:{[tpth]
 tname:last "/"vs string tpth; /splayed table name
 .util.logm"Cataloging splayed table: ",tname;
 querycols:@[{cols x};tpth;{(0b;x)}]; /cols `:path/to/splayed/table
 tblmeta:@[{meta x};tpth;{(0b;x)}]; /meta `:path/to/splayed/table
 dpth:.Q.dd[tpth;`.d];
 dcols:@[{get x};dpth;{(0b;x)}]; /get `:path/to/splayed/table/.d
 dfilegood:11h~type dcols;
 .util.logm".d status: ",string(`CORRUPT`GOOD)dfilegood;
 .util.logm"Number of columns in .d file: ",string count dcols;
 tfiles:(k where not(k:key tpth)like\:"*#")except`.d;
 .util.logm"Number of columns in splayed table directory: ",string count tfiles;
 unusedcols:distinct dcols except tfiles; /columns referenced in the .d file but no associated column file in table dir
 missingcols:distinct tfiles except dcols; /columns within directory but not referenced in the .d file
 dupcols:raze where 1<count each group dcols; /columns which are referenced more than once in the .d file
 goodcols:distinct dcols inter tfiles;
 .util.logm"Gathering metrics for each found column...";
 colmetrics:{catalogueColumn get x}each .Q.dd[tpth;]each goodcols;
 -1"\n";.util.logm"Finished cataloging splayed table: ",tname;
 if[98h~type colmetrics;colmetrics:`colname xcols update colname:goodcols from colmetrics;];
 :`colmetrics`querycols`tblmeta`goodcols`unusedcols`missingcols`dupcols`dfilegood!(colmetrics;querycols;tblmeta;goodcols;unusedcols;missingcols;dupcols;dfilegood);
 }

catalogueParted:{[dbinfo;tname]
 partedinfo:catalogueSplayed each .Q.dd[dbinfo`db;]each dbinfo[`partitions],\:tname;
 :`part xcols @[partedinfo;`part;:;dbinfo[`partitions]];
 }

catalogueBinary:{[tpth]
 tname:last "/"vs string tpth; /splayed table name
 .util.logm"Cataloging binary object: ",tname;
 objdata:@[{get x};tpth;{(0b;x)}]; /get the kdb+ object
 objtype:type objdata; /get the returned object data type
 if[not 98h~type objdata;:(0b;"Not a kdb+ table object")]; /exit and return fail error if object not table
 if[0b~first objdata;:objdata]; /if getting object failed, return with error
 /by here object is a table, so safe to run `cols` and `meta` and `select` - presumably
 tcols:cols objdata;
 .util.logm"Number of columns in binary table: ",string count tcols;
 tmeta:meta objdata;
 colmetrics:{catalogueColumn x[y]}[objdata;]each tcols;
 colmetrics:`colname xcols update colname:tcols from colmetrics;
 -1"\n";.util.logm"Complete cataloging of binary object: ",tname;
 :`colmetrics`querycols`tblmeta!(colmetrics;tcols;tmeta);
 }

catalogueDBObjects:{[dbinfo]
 @[{`sym set get x};.Q.dd[dbinfo[`db];`sym];{(0b;x)}]; /if a sym file exists in the db root dir, load it and store in the global 'sym' variable
 splayedinfo:catalogueSplayed each .Q.dd[dbinfo`db;]each dbinfo`splayed; /catalogue all found splayed tables
 dbinfo[`splayed]!:splayedinfo;
 partedinfo:catalogueParted[dbinfo;]each dbinfo`parted; /catalogue all found partitioned tables
 dbinfo[`parted]!:partedinfo;
 flatinfo:catalogueBinary each .Q.dd[dbinfo`db;]each dbinfo`flat; /catalogue all found binary table objects
 dbinfo[`flat]!:flatinfo;
 :dbinfo;
 }

catalogueDB:{[dbpath]
 .util.logm"Cataloging database: ",sdb:1 _string dbpath;
 allfiles:key dbpath; /all files within db root
 .util.logm"Number of objects to catalogue: ",string count allfiles;
 filetypes:.util.fileInfo .Q.dd[dbpath;]each allfiles; /Object content type
 splayinfo:`$-1_'"/"vs/:ssr[;sdb,"/";""]each system"find ",sdb," -name .d"; /Objects containing .d file
 splayed:raze splayinfo where 1=count'[splayinfo]; /splayed are 1 dir from dbroot
 parted:distinct last each partitions:splayinfo where 2=count'[splayinfo]; /parted are 2 dir from dbroot
 if[not 0~count parted;partitions:distinct partitions[;0];]; /ammend partitions to be a list of distinct partitions
 flats:(where`data~/:filetypes)except`sym,splayed,parted;
 .util.logm"Number of binary objects found: ",string count flats;
 .util.logm"Number of splayed tables found: ",string count splayed;
 .util.logm"Number of partitioned tables found: ",string count parted;
 .util.logm"Number of partitions found: ",string count partitions;
 :`db`sdb`partitions`allobjs`objtypes`splayed`parted`flat!(dbpath;sdb;partitions;allfiles;filetypes;splayed;parted;flats);
 }

run:{
 st:.z.T;
 .util.logm"Cataloging dbs";
 dbinfo:catalogueDBObjects each catalogueDB each DBS;
 dbinfo:`dbname xcols@[dbinfo;`dbname;:;`$"_"sv/:-2#/:"/"vs/:dbinfo[`sdb]];
 saveto:.Q.par[DBI_DB;.z.D;`DB_METRICS];
 .util.logm"Catalogued all databases. Time taken: ",string .z.T-st;
 .util.logm"Storing metrics to: ", 1_string saveto;
 saveto set dbinfo;
 :1b;
 }
//##################################INITIALISE & KICKSTART#################################//
kickstart:{
 runfn:$[DEVMODE; run; @[run;;{.util.logm"ERROR: FAILED: ",x;:0b}] ];
 $[DEVMODE;.util.logm"Running process in DEV mode";.util.logm"Running without debug"];
 res:runfn();
 if[not NOEXIT;exit res];
 }

kickstart[]
