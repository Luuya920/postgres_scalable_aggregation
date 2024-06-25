-- my_extension--1.0.sql
CREATE FUNCTION add_hundred(arg INT) RETURNS INT
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'add_hundred';

CREATE FUNCTION generate_driving_periods(data JSONB, partition INT) RETURNS TEXT
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'generate_driving_periods';


CREATE FUNCTION avg_speed_transfn_optimized(state internal, speeds double precision[])
RETURNS internal
AS 'MODULE_PATHNAME', 'avg_speed_transfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_finalfn_optimized(state internal, speeds double precision[])
RETURNS float8
AS 'MODULE_PATHNAME', 'avg_speed_finalfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_combinefn_optimized(state1 internal, state2 internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'avg_speed_combinefn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_serializefn(state internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'avg_speed_serializefn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_deserializefn(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'avg_speed_deserializefn'
LANGUAGE C IMMUTABLE;

CREATE AGGREGATE avg_speed_optimized(double precision[]) (
    SFUNC = avg_speed_transfn_optimized,
    STYPE = internal,
    FINALFUNC = avg_speed_finalfn_optimized,
    FINALFUNC_EXTRA,
    COMBINEFUNC = avg_speed_combinefn_optimized,
    SERIALFUNC = avg_speed_serializefn,
    DESERIALFUNC = avg_speed_deserializefn,
    PARALLEL = SAFE
);



CREATE OR REPLACE FUNCTION avg_speed_transfn(state DOUBLE PRECISION[], speeds DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION[] AS $$
DECLARE
    i INT;
BEGIN
    IF state IS NULL THEN
        state := ARRAY[0.0, 0.0];  -- state[1] is sum, state[2] is count
    END IF;

    IF speeds IS NOT NULL THEN
        FOR i IN 1..array_length(speeds, 1) LOOP
            state[1] := state[1] + speeds[i];
            state[2] := state[2] + 1;
        END LOOP;
    END IF;

    RETURN state;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION avg_speed_finalfn(state DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
BEGIN
    IF state IS NULL OR state[2] = 0 THEN
        RETURN NULL;
    END IF;
    RETURN state[1] / state[2];
END;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE avg_speed(DOUBLE PRECISION[]) (
    SFUNC = avg_speed_transfn,
    STYPE = DOUBLE PRECISION[],
    FINALFUNC = avg_speed_finalfn,
    INITCOND = '{0.0, 0.0}'
);