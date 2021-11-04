from Scripts.crud import get_last_agency_id, get_last_calendar_id, get_last_route_id, get_last_stop_id, get_last_trip_id

agency_id_count = id_ if (id_ := get_last_agency_id()) is not None else -1
calendar_id_count = id_ if (id_ := get_last_calendar_id()) is not None else -1
route_id_count = id_ if (id_ := get_last_route_id()) is not None else -1
stop_id_count = id_ if (id_ := get_last_stop_id()) is not None else -1
trip_id_count = id_ if (id_ := get_last_trip_id()) is not None else -1
