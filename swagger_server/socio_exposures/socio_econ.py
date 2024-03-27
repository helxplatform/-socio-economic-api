import sys
import json
from configparser import ConfigParser
from datetime import datetime, timedelta
from sqlalchemy import extract, func, cast
from sqlalchemy import select
from sqlalchemy import inspect
from sqlalchemy.orm import load_only
from geoalchemy2 import Geography
import pytz
from flask import jsonify
from swagger_server.models.models import SocioEconomicData2019, CensusBlockGrps2011, CensusBlockGrps2016, CensusBlockGrps2021
from swagger_server.socio_exposures.statics import Statics as s
from swagger_server.controllers import Session
from itertools import zip_longest as zip
from enum import Enum
from datetime import datetime
from sqlalchemy.orm import class_mapper
from sqlalchemy_serializer import SerializerMixin


parser = ConfigParser()
parser.read('swagger_server/ini/connexion.ini')
sys.path.append(parser.get('sys-path', 'exposures'))
sys.path.append(parser.get('sys-path', 'controllers'))


class MeasurementType(Enum):
   # lat: 0 to +/- 90, lon: 0 to +/- 180 as lat,lon

    LATITUDE = '^[+-]?(([1-8]?[0-9])(\.[0-9]+)?|90(\.0+)?)$'
    LONGITUDE = '^[+-]?((([1-9]?[0-9]|1[0-7][0-9])(\.[0-9]+)?)|180(\.0+)?)$'

    def isValid(self, measurement):
        import re
        if re.match(self.value, str(measurement)) is None:
            return False
        else:
            return True



class SocioEconExposures(object):

    def is_valid_lat_lon(self, **kwargs):
        # lat: 0 to +/- 90, lon: 0 to +/- 180 as lat,lon
        if not MeasurementType.LATITUDE.isValid(kwargs.get('latitude')):
            return False, ('Invalid parameter', 400, {'x-error': 'Invalid parameter: latitude'})
        if not MeasurementType.LONGITUDE.isValid(kwargs.get('longitude')):
            return False, ('Invalid parameter', 400, {'x-error': 'Invalid parameter: longitude'})

        return True, ''


    def validate_parameters(self, **kwargs):
        lat_lon_valid, msg = self.is_valid_lat_lon(**kwargs)

        if not lat_lon_valid:
            return False, msg
        else:
            return True, ''


    def get_year_from_range(self, year_range):

        return s.Socio_Econ_Data_Years.get(year_range, "All")


    # get census geoid for specific year range - returns dict because could
    # be different ones if year range is "All"
    def get_census_geoid(self, lat, lon, year):
        geoid = {}

        if (year == "All"):
            for year_value in s.Socio_Econ_Data_Years.values():
                census_obj_name = "CensusBlockGrps" + year_value
                census_obj = globals()[census_obj_name]

                session = Session()
                # given this lat lon, find the census tract that contains it.
                query = session.query(census_obj.geoid). \
                                filter(func.ST_Contains(census_obj.geom,
                                        func.ST_GeomFromText("POINT(" + str(lon) + " " + str(lat) + ")", 4269)))
                result = session.execute(query, execution_options={"prebuffer_rows": True})
                for query_return_values in result:
                    tmp_geoid = query_return_values[0]

                if (len(tmp_geoid) > 14):
                    geoid[year_value] = tmp_geoid[-14:]
                else:
                    geoid[year_value] = tmp_geoid

                session.close()

        else:
            census_obj_name = "CensusBlockGrps" + year
            census_obj = globals()[census_obj_name]

            session = Session()
            # given this lat lon, find the census tract that contains it.
            query = session.query(census_obj.geoid). \
                            filter(func.ST_Contains(census_obj.geom,
                                   func.ST_GeomFromText("POINT(" + str(lon) + " " + str(lat) + ")", 4269)))
            result = session.execute(query, execution_options={"prebuffer_rows": True})
            for query_return_values in result:
                tmp_geoid = query_return_values[0]

            if (len(tmp_geoid) > 14):
                geoid[year] = tmp_geoid[-14:]
            else:
                geoid[year] = tmp_geoid

            session.close()

        return geoid


    def get_socio_econ_data(self, session, year, geoid):

        cols = s.Socio_Econ_Data_Columns[year].keys()

        statement = select(SocioEconomicData2019).filter_by(bgid2016=geoid[year])
        result = session.execute(statement)

        return result


    def get_values(self, **kwargs):
        # latitude, longitude, years

        # validate input from user
        is_valid, message = self.validate_parameters(**kwargs)
        if not is_valid:
            return message

        # create data object
        data = {}
        data['values'] = []

        # retrieve query result for lat,lon pair and year range and add to data object
        lat = kwargs.get('latitude')
        lon = kwargs.get('longitude')
        year_range = kwargs.get('years')

        year = self.get_year_from_range(year_range)
        geoid = self.get_census_geoid(lat, lon, year)
        data.update({'latitude': lat, 'longitude': lon})

        session = Session(future=True)
        for year_key in geoid:
            result = self.get_socio_econ_data(session, year_key, geoid)

            for query_return_values in result:

                # change any null values to 'n/a'
                val_dict = {}
                new_query_return_list = []
                for val in query_return_values:
                    if(val is None):
                        new_query_return_list.append("n/a")
                    else:
                        val_dict = val.to_dict()
                        new_query_return_list.append(val_dict)

                # Create a dict from values of mapped static column values (as keys) and new_query_return_list (as values)
                tmp_dict = {key: val for key, val in zip(s.Socio_Econ_Data_Columns[year_key].values(), new_query_return_list)}
                tmp_dict_2019 = tmp_dict.pop('id')
                year_cols_dict = s.Socio_Econ_Data_Columns[year_key]
                print(13412340912384192308, tmp_dict, year_cols_dict)
                for k, v in tmp_dict_2019.items():
                    try:
                        if tmp_dict.__contains__(year_cols_dict[k]):
                            tmp_dict[year_cols_dict[k]] = v
                    except KeyError:
                        pass

                # change format of geoid
                if tmp_dict["geoid"] is not None:
                    tmp_dict["geoid"] = '15000US' + tmp_dict["geoid"]

                # add year range ie reverse lookup year-range based on year sent in
                for dkey, dvalue in s.Socio_Econ_Data_Years.items():
                    if(dvalue == year_key):
                        dict_key = dkey
                        break
                tmp_dict.update({"years": dict_key})

                data['values'].append(tmp_dict)

                break

        session.commit()
        session.close()
        return jsonify(data)
