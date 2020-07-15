-- Create the tables to be used in the level 2 GCN.


-- Delete the tables if they already exist.
drop table if exists missions cascade;
drop table if exists notices cascade;
drop table if exists details cascade;

-- Create the tables.
create table missions (
    mid          int primary key,     -- A unique identifier for the mission
    name         text,   -- The name of the mission
    description  text,   -- A description of the mission
    basedir      text   -- A directory in which notices associated with this mission will be found
); -- missions

create table notices (
    nid         int primary key,      
                         -- A unique identifier for the notice
    mid         int references missions(mid),     
                         -- The mission associated with the notice
    file        text    -- The file name (sans directory where the notice is stored
); -- notices

create table details (
    nid         int references notices(nid),     
                         -- The notice the detail is from
    line        int,     -- The line in the notice
    key         text,    -- The keyword for this line
    textval     text,    -- The value as a text string (always set)
    realval     double precision,   
                         -- The value as a double (set for all numerics)
    arrval      double precision array  
                         -- The value as a double array (set for array valued numerics)
); -- details

-- All the public to query them.
grant select on missions to public;
grant select on notices to public;
grant select on details to public;