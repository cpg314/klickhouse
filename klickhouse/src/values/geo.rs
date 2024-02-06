//! Geo types
//! <https://clickhouse.com/docs/en/sql-reference/data-types/geo>
use super::*;

use itertools::Itertools;

#[derive(Clone, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Geo point, represented by its x and y coordinates.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#point>
pub struct Point(pub [f64; 2]);
impl std::fmt::Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({},{})", self.0[0], self.0[1])
    }
}

impl std::hash::Hash for Point {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.0 {
            x.to_bits().hash(state);
        }
    }
}
impl std::ops::Index<u8> for Point {
    type Output = f64;
    fn index(&self, index: u8) -> &Self::Output {
        &self.0[index as usize]
    }
}
impl AsRef<[f64; 2]> for Point {
    fn as_ref(&self) -> &[f64; 2] {
        &self.0
    }
}

macro_rules! display_recurse {
    ($t: ty) => {
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "[{}]", self.0.iter().map(|x| x.to_string()).join(","))
            }
        }
    };
}

#[derive(Clone, Hash, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Polygon without holes.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#ring>
pub struct Ring(pub Vec<Point>);
display_recurse!(Ring);

#[derive(Clone, Hash, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Polygon with holes. The first element is the outer polygon, and the following ones are the holes.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#polygon>
pub struct Polygon(pub Vec<Ring>);
display_recurse!(Polygon);

#[derive(Clone, Hash, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Union of polygons.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#multipolygon>
pub struct MultiPolygon(pub Vec<Polygon>);
display_recurse!(MultiPolygon);

macro_rules! to_from_sql {
    ($name:ident) => {
        impl ToSql for $name {
            fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
                Ok(Value::$name(self))
            }
        }

        impl FromSql for $name {
            fn from_sql(type_: &Type, value: Value) -> Result<Self> {
                if !matches!(type_, Type::$name) {
                    return Err(unexpected_type(type_));
                }
                match value {
                    Value::$name(x) => Ok(x),
                    _ => unimplemented!(),
                }
            }
        }
    };
}

to_from_sql!(Point);
to_from_sql!(Ring);
to_from_sql!(Polygon);
to_from_sql!(MultiPolygon);
#[cfg(feature = "geo-types")]
mod nav_types_conversions {
    use super::*;

    macro_rules! to_from_sql {
        ($geo_t:path, $ch_t:ident) => {
            impl ToSql for $geo_t {
                fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
                    Ok(Value::$ch_t(self.into()))
                }
            }
            impl FromSql for $geo_t {
                fn from_sql(type_: &Type, value: Value) -> Result<Self> {
                    if !matches!(type_, Type::$ch_t) {
                        return Err(unexpected_type(type_));
                    }
                    match value {
                        Value::$ch_t(x) => Ok(x.into()),
                        _ => unimplemented!(),
                    }
                }
            }
        };
    }
    // Points and coords
    impl From<Point> for geo_types::Coord {
        fn from(source: Point) -> Self {
            Self {
                x: source[0],
                y: source[1],
            }
        }
    }
    impl From<geo_types::Coord> for Point {
        fn from(source: geo_types::Coord) -> Self {
            Self([source.x, source.y])
        }
    }
    to_from_sql!(geo_types::Coord, Point);

    // Points and points
    impl From<Point> for geo_types::Point {
        fn from(source: Point) -> Self {
            geo_types::Point(source.into())
        }
    }
    impl From<geo_types::Point> for Point {
        fn from(source: geo_types::Point) -> Self {
            source.0.into()
        }
    }
    to_from_sql!(geo_types::Point, Point);
    // Rings and Linestrings
    impl From<Ring> for geo_types::LineString {
        fn from(source: Ring) -> Self {
            Self(source.0.into_iter().map(geo_types::Coord::from).collect())
        }
    }
    impl From<geo_types::LineString> for Ring {
        fn from(source: geo_types::LineString) -> Self {
            Self(source.0.into_iter().map(Point::from).collect())
        }
    }
    to_from_sql!(geo_types::LineString, Ring);
    // Rings and polygons (with no holes)
    // A Polygon -> Ring conversion is not provided, as the polygon might have holes.
    impl From<Ring> for geo_types::Polygon {
        fn from(source: Ring) -> Self {
            geo_types::Polygon::new(source.0.into(), vec![])
        }
    }
    // Polygons and polygons
    impl From<geo_types::Polygon> for Polygon {
        fn from(source: geo_types::Polygon) -> Self {
            Self(
                [source.exterior().clone().into()]
                    .into_iter()
                    .chain(
                        source
                            .interiors()
                            .iter()
                            .map(|linestring| Ring::from(linestring.clone())),
                    )
                    .collect(),
            )
        }
    }
    impl From<Polygon> for geo_types::Polygon {
        fn from(mut source: Polygon) -> Self {
            if source.0.is_empty() {
                return Self::new(geo_types::LineString::new(vec![]), vec![]);
            }
            let exterior = source.0.remove(0);
            geo_types::Polygon::new(
                exterior.into(),
                source
                    .0
                    .into_iter()
                    .map(geo_types::LineString::from)
                    .collect(),
            )
        }
    }
    to_from_sql!(geo_types::Polygon, Polygon);
    // Multi polygons
    impl From<MultiPolygon> for geo_types::MultiPolygon {
        fn from(source: MultiPolygon) -> Self {
            source.0.into_iter().map(geo_types::Polygon::from).collect()
        }
    }
    impl From<geo_types::MultiPolygon> for MultiPolygon {
        fn from(source: geo_types::MultiPolygon) -> Self {
            Self(source.into_iter().map(Polygon::from).collect())
        }
    }
    to_from_sql!(geo_types::MultiPolygon, MultiPolygon);
    #[cfg(test)]
    #[test]
    fn roundtrip() {
        let multipolygon_geo: geo_types::MultiPolygon = geo_types::wkt! {
            // Example from https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
            MULTIPOLYGON (((40.0 40.0, 20.0 45.0, 45.0 30.0, 40.0 40.0)),
                          ((20.0 35.0, 10.0 30.0, 10.0 10.0, 30.0 5.0, 45.0 20.0, 20.0 35.0),
                           (30.0 20.0, 20.0 15.0, 20.0 25.0, 30. 20.0)))
        };
        let multipolygon = MultiPolygon::from(multipolygon_geo.clone());
        let multipolygon_geo2 = geo_types::MultiPolygon::from(multipolygon);
        assert_eq!(multipolygon_geo, multipolygon_geo2);
    }
}
