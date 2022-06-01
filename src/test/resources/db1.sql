      -- HSQL DB CREATE SCRIPT --

CREATE TABLE contact (
    id integer NOT NULL,
    name varchar(50),
    sex varchar(1),
    birth_date date,
    email varchar(50)
)
//
CREATE TABLE notes (
  id integer NOT NULL,
  contact_id integer NOT NULL,
  note_date timestamp,
  note varchar(250)
)
//
CREATE TABLE visit (
  id integer NOT NULL,
  contact_id integer NOT NULL,
  visit_date date NOT NULL,
  visit_time time NOT NULL,
)
alter table contact add primary key (id)
//
alter table notes add primary key (id)
//
alter table notes add foreign key (contact_id) references contact(id)
//
alter table visit add primary key (id)
//
alter table visit add foreign key (contact_id) references contact(id)
CREATE SEQUENCE seq1 START WITH 100000
