use polars::export::arrow::io::parquet::write::{
    CompressionOptions, GzipLevel, Version, WriteOptions,
};

pub fn write_options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        version: Version::V2,
        compression: CompressionOptions::Gzip(Some(GzipLevel::default())),
        data_pagesize_limit: None,
    }
}

#[cfg(test)]
mod tests {
    use polars::export::arrow::array::{
        Array, DictionaryArray, DictionaryKey, PrimitiveArray, Utf8Array,
    };
    use polars::export::arrow::datatypes::DataType;

    #[test]
    fn try_new_ok() {
        let values = Utf8Array::<i32>::from_slice(["a", "aa"]);
        let data_type =
            DataType::Dictionary(i32::KEY_TYPE, Box::new(values.data_type().clone()), false);
        let array = DictionaryArray::try_new(
            data_type,
            PrimitiveArray::from_vec(vec![1, 0]),
            values.boxed(),
        )
        .unwrap();

        assert_eq!(array.keys(), &PrimitiveArray::from_vec(vec![1i32, 0]));
        assert_eq!(
            &Utf8Array::<i32>::from_slice(["a", "aa"]) as &dyn Array,
            array.values().as_ref(),
        );
        assert!(!array.is_ordered());

        assert_eq!(format!("{array:?}"), "DictionaryArray[aa, a]");
    }
}
