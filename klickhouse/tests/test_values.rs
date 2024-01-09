//! This test check whether the string interpretations of [`klickhouse::Value`] is correct, in the
//! sense that it should be interpreted by Clickhouse as the correct type and value.

async fn test_one<
    T: Clone + std::fmt::Debug + std::cmp::PartialEq + klickhouse::ToSql + klickhouse::FromSql,
>(
    sample: T,
    client: &klickhouse::Client,
) {
    println!("{}", sample.clone().to_sql(None).unwrap());
    let sample2: klickhouse::UnitValue<T> = client
        .query_one(klickhouse::QueryBuilder::new("SELECT $1").arg(sample.clone()))
        .await
        .unwrap();
    assert_eq!(sample, sample2.0);
}

#[tokio::test]
async fn values_display() {
    let client = super::get_client().await;

    // TODO: Other Value variants

    // UUID
    test_one(klickhouse::Uuid::new_v4(), &client).await;

    // Date/time
    test_one(
        klickhouse::DateTime::try_from(chrono::Utc::now()).unwrap(),
        &client,
    )
    .await;
    test_one(
        klickhouse::Date::from(chrono::NaiveDate::from_ymd_opt(2015, 3, 14).unwrap()),
        &client,
    )
    .await;

    // Geotypes
    let point = klickhouse::Point([1.0, 2.0]);
    test_one(point.clone(), &client).await;
    let ring = klickhouse::Ring(vec![point]);
    test_one(ring.clone(), &client).await;
    let polygon = klickhouse::Polygon(vec![ring]);
    test_one(polygon.clone(), &client).await;
    test_one(klickhouse::MultiPolygon(vec![polygon]), &client).await;
}
