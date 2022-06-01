CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE IF NOT EXISTS "gauges" (
	"gauge_pk"	INTEGER,
	"gauge_id"	TEXT UNIQUE,
	"gauge_location"	TEXT,
	"gauge_tz"	INTEGER,
	PRIMARY KEY("gauge_pk")
);
CREATE TABLE IF NOT EXISTS "_observations_old" (
	"observation_time"	TEXT,
	"observation_stage"	REAL,
	"observation_flow"	REAL,
	"observation_pk"	INTEGER NOT NULL UNIQUE,
	"observation_gauge_id"	INTEGER,
	PRIMARY KEY("observation_pk" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "projections" (
	"projection_time_added"	TEXT,
	"projection_time"	TEXT,
	"projection_stage"	INTEGER,
	"projection_flow"	INTEGER,
	"projection_pk"	INTEGER NOT NULL UNIQUE,
	"projection_gauge_id"	INTEGER,
	PRIMARY KEY("projection_pk")
);
CREATE TABLE observations
( "observation_pk" INTEGER NOT NULL UNIQUE,
  "observation_time" TEXT,
 "observation_stage" REAL, 
 "observation_flow" REAL, 
  "observation_gauge_id" INTEGER, 
  CONSTRAINT fk_departments
    FOREIGN KEY (observation_gauge_id)
    REFERENCES gauges(gauge_id)
       ON UPDATE RESTRICT
       ON DELETE RESTRICT,
  PRIMARY KEY("observation_pk" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "forecast" (
	"forecast_pk"	INTEGER NOT NULL UNIQUE,
	"forecast_time_added"	TEXT,
	"forecast_time"	TEXT,
	"forecast_stage"	REAL,
	"forecast_flow"	REAL,
	"forecast_gauge_id"	INTEGER,
	PRIMARY KEY("forecast_pk" AUTOINCREMENT),
	CONSTRAINT "forecast_gauge_id" FOREIGN KEY("forecast_gauge_id") REFERENCES "gauges"("gauge_id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
