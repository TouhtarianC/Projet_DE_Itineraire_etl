""" Module to manage Annex table
"""

import uuid
from lib.create_DB_ORM import TourType, TrailType, ThemeTrail, \
    TargetAudience, TrailViz
from sqlalchemy import exc, select
import re

DEBUG = False


def retrieve_annex_modality(session):
    """ search for existing modality into Annex Tables for
        - TOUR_TYPE
        - TRAIL_TYPE
        - THEME
        - TARGET_AUDIENCE
        Return 4 list of respective objects
    """
    tour_type = session.execute(select(TourType)).all()
    trail_type = session.execute(select(TrailType)).all()
    theme = session.execute(select(ThemeTrail)).all()
    target_audience = session.execute(select(TargetAudience)).all()
    return tour_type, trail_type, theme, target_audience


def retrieve_tour_type(session):
    """ search for existing modality into Annex Tables for
        - TOUR_TYPE
    """
    return session.execute(select(TourType)).all()


def retrieve_trail_type(session):
    """ search for existing modality into Annex Tables for
        - TRAIL_TYPEE
    """
    return session.execute(select(TrailType)).all()


def retrieve_theme(session):
    """ search for existing modality into Annex Tables for
        - THEME
    """
    return session.execute(select(ThemeTrail)).all()


def retrieve_target_audience(session):
    """ search for existing modality into Annex Tables for
        - TARGET_AUDIENCE
    """
    return session.execute(select(TargetAudience)).all()


def create_tour_type(session, t_type):
    """function to create TOUR_TYPE into DB and return the object"""
    UUID_gen = str(uuid.uuid4())
    t_tour = TourType(
        id=UUID_gen,
        NAME=t_type
    )
    try:
        # creation of row into DB MariaDB from object
        session.add(t_tour)
        # session.commit()
        print(f"new TOUR_TYPE created : {t_tour.NAME}")
        return t_tour
    except exc.IntegrityError:
        session.rollback()
        print(f"Integrity error on TOUR_TYPE add: {t_tour.NAME} \
              wasn't created")


def create_trail_type(session, t_type):
    """function to create TRAIL_TYPE into DB and return the object"""
    UUID_gen = str(uuid.uuid4())
    t_tour = TrailType(
        id=UUID_gen,
        NAME=t_type
    )
    try:
        # creation of row into DB MariaDB from object
        session.add(t_tour)
        # session.commit()
        print(f"new TRAIL_TYPE created : {t_tour.NAME}")
        return t_tour
    except exc.IntegrityError:
        session.rollback()
        print(f"Integrity error on TRAIL_TYPE add: {t_tour.NAME} \
              wasn't created")


def create_theme(session, theme):
    """function to create THEME into DB and return the object"""
    UUID_gen = str(uuid.uuid4())
    t_theme = ThemeTrail(
        id=UUID_gen,
        NAME=theme
    )
    try:
        # creation of row into DB MariaDB from object
        session.add(t_theme)
        # session.commit()
        print(f"new THEME created : {t_theme.NAME}")
        return t_theme
    except exc.IntegrityError:
        session.rollback()
        print(f"Integrity error on THEME add: {t_theme} wasn't created")


def create_audience(session, audience):
    """function to create TARGET_AUDIENCE into DB and return the object"""
    UUID_gen = str(uuid.uuid4())
    t_audience = TargetAudience(
        id=UUID_gen,
        NAME=audience
    )
    try:
        # creation of row into DB MariaDB from object
        session.add(t_audience)
        # session.commit()
        print(f"new TARGET_AUDIENCE created : {t_audience.NAME}")
        return t_audience
    except exc.IntegrityError:
        session.rollback()
        print(f"Integrity error on AUDIANCE add: {t_audience.NAME} \
              wasn't created")


def create_trailViz(session, viz):
    """function to create TRAIL_VISUALIzATION into DB and return the object"""
    # for viz in traiViz_list:
    UUID_gen = str(uuid.uuid4())
    try:
        t_viz = TrailViz(
            id=UUID_gen,
            FILE_TYPE=re.search(r'\.([a-zA-Z0-9]+)$', viz).group(1).lower(),
            FILE_LINK=viz
        )
        try:
            # creation of row into DB MariaDB from object
            session.add(t_viz)
            # session.commit()
            # print(f"new TRAIL_VIZ {t_viz} created")
            return t_viz
        except exc.IntegrityError:
            session.rollback()
            print("Integrity error on TRAIL_VIZ creation")
    except AttributeError:
        # issue with format of file into Datatourisme
        pass
