from typing import List
from sqlalchemy import ForeignKey, String, Column, Table, Integer, \
    Boolean, Float
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import uuid


class Base(DeclarativeBase):
    pass


association_TourType_Trail = Table(
    "association_TourType_Trail",
    Base.metadata,
    Column("TRAIL", ForeignKey("TRAIL.id"), primary_key=True),
    Column("TOUR_TYPE", ForeignKey("TOUR_TYPE.id"), primary_key=True),
)


class TourType(Base):
    __tablename__ = "TOUR_TYPE"
    id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                    default=str(uuid.uuid4()), index=True)
    NAME: Mapped[str] = mapped_column(String(30), unique=True)
    TRAILS: Mapped[List['Trail']] = relationship(
        'Trail',
        secondary=association_TourType_Trail,
        back_populates='TOUR_TYPE')

    def __repr__(self) -> str:
        return f"TourType(id={self.id!r}, NAME={self.NAME!r})"
    
    def __init__(self, id, NAME):
        self.id = id
        self.NAME = NAME


association_TrailType_Trail = Table(
    "association_TrailType_Trail",
    Base.metadata,
    Column("TRAIL", ForeignKey("TRAIL.id"), primary_key=True),
    Column("TRAIL_TYPE", ForeignKey("TRAIL_TYPE.id"), primary_key=True),
)


class TrailType(Base):
    __tablename__ = "TRAIL_TYPE"
    id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                    default=str(uuid.uuid4()), index=True)
    NAME: Mapped[str] = mapped_column(String(30), unique=True)
    TRAILS: Mapped[List['Trail']] = relationship(
        'Trail',
        secondary=association_TrailType_Trail,
        back_populates='TRAIL_TYPE')

    def __repr__(self) -> str:
        return f"TrailType(id={self.id!r}, NAME={self.NAME!r})"
    
    def __init__(self, id, NAME):
        self.id = id
        self.NAME = NAME


association_ThemeTrail_Trail = Table(
    "association_Theme_Trail",
    Base.metadata,
    Column("TRAIL", ForeignKey("TRAIL.id"), primary_key=True),
    Column("THEME", ForeignKey("THEME.id"), primary_key=True),
)


class ThemeTrail(Base):
    __tablename__ = "THEME"
    id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                    default=str(uuid.uuid4()), index=True)
    NAME: Mapped[str] = mapped_column(String(30), unique=True)
    TRAILS: Mapped[List['Trail']] = relationship(
        'Trail',
        secondary=association_ThemeTrail_Trail,
        back_populates='THEME')

    def __repr__(self) -> str:
        return f"Theme(id={self.id!r}, NAME={self.NAME!r})"
    
    def __init__(self, id, NAME):
        self.id = id
        self.NAME = NAME


association_Audience_Trail = Table(
    "association_Audience_Trail",
    Base.metadata,
    Column("TRAIL", ForeignKey("TRAIL.id"), primary_key=True),
    Column("TARGET_AUDIENCE", ForeignKey("TARGET_AUDIENCE.id"),
           primary_key=True),
)

association_Audience_Poi = Table(
    "association_Audience_Poi",
    Base.metadata,
    Column("poi_id", ForeignKey("POI.id")),
    Column("target_audience_id", ForeignKey("TARGET_AUDIENCE.id")),
)


# class TargetAudience(Base):
#     __tablename__ = "TARGET_AUDIENCE"
#     id: Mapped[str] = mapped_column(String(36), primary_key=True,
#                                     default=str(uuid.uuid4()), index=True)
#     AUDIENCE: Mapped[str] = mapped_column(String(30), unique=True)

#     def __repr__(self) -> str:
#         return f"TargetAudience(id={self.id!r}, AUDIENCE={self.AUDIENCE!r})"

class TargetAudience(Base):
    __tablename__ = "TARGET_AUDIENCE"
    id: Mapped[str] = Column(String(36), primary_key=True,
                             default=str(uuid.uuid4()), index=True)
    NAME: Mapped[str] = Column(String(255), unique=True)
    POIS: Mapped[List['Poi']] = relationship(
        'Poi',
        secondary=association_Audience_Poi,
        back_populates='TARGET_AUDIENCE')
    TRAILS: Mapped[List['Trail']] = relationship(
        'Trail',
        secondary=association_Audience_Trail,
        back_populates='TARGET_AUDIENCE')

    def __repr__(self) -> str:
        return f"TargetAudience(id={self.id!r}, NAME={self.NAME!r})"

    def __init__(self, id, NAME):
        self.id = id
        self.NAME = NAME

association_Viz_Trail = Table(
    "association_Viz_Trail",
    Base.metadata,
    Column("TRAIL", ForeignKey("TRAIL.id"), primary_key=True),
    Column("TRAIL_VISUALIZATION", ForeignKey("TRAIL_VISUALIZATION.id"),
           primary_key=True),
)


class TrailViz(Base):
    __tablename__ = "TRAIL_VISUALIZATION"
    id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                    default=str(uuid.uuid4()), index=True)
    FILE_TYPE: Mapped[str] = mapped_column(String(30))
    FILE_LINK: Mapped[str] = mapped_column(String(255))
    TRAILS: Mapped[List['Trail']] = relationship(
        'Trail',
        secondary=association_Viz_Trail,
        back_populates='TRAIL_VISUALIZATION')

    def __repr__(self) -> str:
        return f"TrailViz(id={self.id!r}, FILE_TYPE={self.FILE_TYPE!r}, \
            FILE_LINK={self.FILE_LINK!r})"
    
    def __init__(self, id, FILE_TYPE, FILE_LINK):
        self.id = id
        self.FILE_TYPE = FILE_TYPE
        self.FILE_LINK = FILE_LINK


class Trail(Base):
    __tablename__ = "TRAIL"
    id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                    default=str(uuid.uuid4()), index=True)
    DataTourism_ID: Mapped[str] = mapped_column(String(255), index=True,
                                                unique=True)
    TOUR_TYPE: Mapped[List["TourType"]] = relationship(
        secondary=association_TourType_Trail, back_populates='TRAILS')
    TRAIL_TYPE: Mapped[List["TrailType"]] = relationship(
        secondary=association_TrailType_Trail, back_populates='TRAILS')
    THEME: Mapped[List["ThemeTrail"]] = relationship(
        secondary=association_ThemeTrail_Trail, back_populates='TRAILS')
    TARGET_AUDIENCE: Mapped[List["TargetAudience"]] = relationship(
        secondary=association_Audience_Trail, back_populates='TRAILS')
    PETS_ALLOWED: Mapped[bool] = Column(Boolean, default=None)
    DURATION: Mapped[float] = Column(Float, default=0)
    DISTANCE: Mapped[float] = Column(Float, default=0)
    TRAIL_VISUALIZATION: Mapped[List["TrailViz"]] = relationship(
        secondary=association_Viz_Trail, back_populates='TRAILS')
    IMAGE_LINK: Mapped[str] = Column(String(255))
    CITY: Mapped[str] = Column(String(60))
    POSTAL_CODE: Mapped[int] = Column(Integer, index=True)
    POSTAL_ADDRESS: Mapped[str] = Column(String(255))
    LAST_UPDATE: Mapped[str] = Column(String(12))

    def __repr__(self) -> str:
        return f"Trail(id={self.id!r},\
                    DataTourism_ID={self.DataTourism_ID!r},\
                    TOUR_TYPE={self.TOUR_TYPE!r},\
                    TRAIL_TYPE={self.TRAIL_TYPE!r},\
                    THEME={self.THEME!r},\
                    TARGET_AUDIENCE={self.TARGET_AUDIENCE!r},\
                    PETS_ALLOWED={self.PETS_ALLOWED!r},\
                    DURATION={self.DURATION!r},\
                    DISTANCE={self.DISTANCE!r},\
                    TRAIL_VISUALIZATION={self.TRAIL_VISUALIZATION!r},\
                    IMAGE_LINK={self.IMAGE_LINK!r},\
                    CITY={self.CITY!r},\
                    POSTAL_CODE={self.POSTAL_CODE!r},\
                    POSTAL_ADDRESS={self.POSTAL_ADDRESS!r},\
                    LAST_UPDATE={self.LAST_UPDATE!r},\
                    )"


association_PoiType_Poi = Table(
    "association_PoiType_Poi",
    Base.metadata,
    Column("poi_id", ForeignKey("POI.id")),
    Column("poi_type_id", ForeignKey("POI_TYPE.id")),
)


association_PoiTheme_Poi = Table(
    "association_PoiTheme_Poi",
    Base.metadata,
    Column("poi_id", ForeignKey("POI.id")),
    Column("poi_theme_id", ForeignKey("POI_THEME.id")),
)


class PoiType(Base):
    __tablename__ = "POI_TYPE"
    id: Mapped[str] = Column(String(36),
                             primary_key=True,
                             default=str(uuid.uuid4()),
                             index=True)
    NAME: Mapped[str] = Column(String(30), unique=True)
    POIS: Mapped[List['Poi']] = relationship('Poi',
                                             secondary=association_PoiType_Poi,
                                             back_populates='POI_TYPES')

    def __repr__(self) -> str:
        return f"PoiType(id={self.id!r}, NAME={self.NAME!r})"

    def __init__(self, NAME):
        self.id = str(uuid.uuid4())
        self.NAME = NAME


class PoiTheme(Base):
    __tablename__ = "POI_THEME"
    id: Mapped[str] = Column(String(36),
                             primary_key=True,
                             default=str(uuid.uuid4()),
                             index=True)
    NAME: Mapped[str] = Column(String(30), unique=True)
    POIS: Mapped[List['Poi']] = relationship(
        'Poi',
        secondary=association_PoiTheme_Poi,
        back_populates='POI_THEMES')

    def __repr__(self) -> str:
        return f"PoiTheme(id={self.id!r}, NAME={self.NAME!r})"
    
    def __init__(self, NAME):
        self.id = str(uuid.uuid4())
        self.NAME = NAME


class Poi(Base):
    __tablename__ = "POI"
    id: Mapped[str] = Column(String(36),
                             primary_key=True,
                             default=str(uuid.uuid4()),
                             index=True)
    PETS_ALLOWED: Mapped[bool] = Column(Boolean, default=None)
    REDUCED_MOBILITY_ACCESS: Mapped[bool] = Column(Boolean, default=None)
    WEBPAGE_LINK: Mapped[str] = Column(String(255), default=None)
    IMAGE_LINK: Mapped[str] = Column(String(255), default=None)
    CITY: Mapped[str] = Column(String(60), default=None)
    POSTAL_CODE: Mapped[int] = Column(Integer, default=None)
    POSTAL_ADDRESS: Mapped[str] = Column(String(255), default=None)
    POI_TYPES: Mapped[List['PoiType']] = relationship(
        'PoiType',
        secondary=association_PoiType_Poi,
        back_populates='POIS')
    POI_THEMES: Mapped[List['PoiTheme']] = relationship(
        'PoiTheme',
        secondary=association_PoiTheme_Poi,
        back_populates='POIS')
    TARGET_AUDIENCE: Mapped[List['TargetAudience']] = relationship(
        'TargetAudience',
        secondary=association_Audience_Poi,
        back_populates='POIS')
    DATATOURISME_ID: Mapped[str] = Column(String(255), index=True)
    LAST_UPDATE: Mapped[str] = Column(String(24))

    def __repr__(self) -> str:
        return f"Poi(id={self.id!r}, \
                    PETS_ALLOWED={self.PETS_ALLOWED!r}, \
                    REDUCED_MOBILITY_ACCESS={self.REDUCED_MOBILITY_ACCESS!r}, \
                    WEBPAGE_LINK={self.WEBPAGE_LINK!r}, \
                    IMAGE_LINK={self.IMAGE_LINK!r}, \
                    CITY={self.CITY!r}, \
                    POSTAL_CODE={self.POSTAL_CODE!r}, \
                    POSTAL_ADDRESS={self.POSTAL_ADDRESS!r}, \
                    POI_TYPES={self.POI_TYPES!r}, \
                    POI_THEMES={self.POI_THEMES!r}, \
                    TARGET_AUDIENCE={self.TARGET_AUDIENCE!r}, \
                    DATATOURISME_ID={self.DATATOURISME_ID!r}, \
                    LAST_UPDATE={self.LAST_UPDATE!r}, \
                    )"
    
    def __init__(self, id, PETS_ALLOWED, REDUCED_MOBILITY_ACCESS, WEBPAGE_LINK, IMAGE_LINK,
                 CITY, POSTAL_CODE, POSTAL_ADDRESS, DATATOURISME_ID, LAST_UPDATE):
        self.id=id
        self.PETS_ALLOWED=PETS_ALLOWED
        self.REDUCED_MOBILITY_ACCESS=REDUCED_MOBILITY_ACCESS
        self.WEBPAGE_LINK=WEBPAGE_LINK
        self.IMAGE_LINK=IMAGE_LINK
        self.CITY=CITY
        self.POSTAL_CODE=POSTAL_CODE
        self.POSTAL_ADDRESS=POSTAL_ADDRESS
        self.DATATOURISME_ID=DATATOURISME_ID
        self.LAST_UPDATE=LAST_UPDATE