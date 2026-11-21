CREATE FUNCTION osm.node_ids(b bytea) RETURNS int8[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    r int8[] := '{}';
    v int8 := 0;
    prev int8 := 0;
    shift int := 0;
    byte int;
BEGIN
    FOR i IN 0 .. length(b) - 1 LOOP
        byte := get_byte(b, i);
        v := v | ((byte & 127)::int8 << shift);
        IF byte < 128 THEN
            -- Zigzag decoding; `>>` keeps the sign, so the mask clears the top bit as an unsigned shift would
            prev := prev + (((v >> 1) & 9223372036854775807) # -(v & 1));
            r := r || prev;
            v := 0;
            shift := 0;
        ELSE
            shift := shift + 7;
        END IF;
    END LOOP;
    RETURN r;
END $$;
