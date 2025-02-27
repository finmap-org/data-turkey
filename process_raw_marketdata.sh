function create_datafiles() {
  local date=$1
  local inputfile=$2
  local sectors=$3

  local datadir="data/$( echo "${date}" | sed 's/-/\//g' )"
  local outputfile="${datadir}/bist.json"

  if [ -f "${outputfile}" ]; then
    echo "File already exists: ${outputfile}"
    return;
  fi

  mkdir -p "${datadir}"

  jq --arg date "${date}" \
   --argjson sectors "${sectors}" \
   -R '
   # Datafiles may have different columns order
   # so we need to process headers first
   [inputs] as $raw_lines |               # First read all lines as raw strings
   ($raw_lines | map(sub("^\uFEFF";"")))  # Remove BOM from all lines
   | map(split(";"))                      # Then split each line by semicolon
   as $lines |
   ($lines[0] | to_entries | map({key: .value, value: .key}) | from_entries) as $headers |
   ($headers | {
     date_idx: .["TRADE DATE"],
     ticker_idx: .["INSTRUMENT SERIES CODE"],
     name_idx: .["INSTRUMENT NAME"],
     type_idx: .["INSTRUMENT TYPE"],
     open_idx: .["OPENING PRICE"],
     last_idx: .["CLOSING PRICE"],
     change_idx: .["CHANGE TO PREVIOUS CLOSING (%)"],
     value_idx: .["TOTAL TRADED VALUE"],
     volume_idx: .["TOTAL TRADED VOLUME"],
     trades_idx: .["TOTAL NUMBER OF CONTRACTS"]
   }) as $idx |
   $lines[2:][] |                          # Process data lines (after two headers)
   . as $fields |
   ($fields[$idx.ticker_idx] | match("^[A-Za-z0-9]+").string) as $ticker |
   ($sectors[$ticker][0] // $fields[$idx.type_idx]) as $sector |
   ($sectors[$ticker][1] // $fields[$idx.name_idx]) as $name |
   select($fields[$idx.date_idx] == $date) |
   [
     "BIST",
     "",
     $fields[$idx.type_idx],
     $sector,
     $fields[$idx.type_idx],
     "",
     $fields[$idx.ticker_idx],
     $fields[$idx.name_idx],
     $name,
     $fields[$idx.name_idx],
     $name,
     (if $fields[$idx.open_idx] == "" then 0 else ($fields[$idx.open_idx] | tonumber) end),
     (if $fields[$idx.last_idx] == "" then 0 else ($fields[$idx.last_idx] | tonumber) end),
     (if $fields[$idx.change_idx] == "" then 0 else ($fields[$idx.change_idx] | tonumber) end),
     (if $fields[$idx.volume_idx] == "" then 0 else ($fields[$idx.volume_idx] | tonumber) end),
     (if $fields[$idx.value_idx] == "" then 0 else ($fields[$idx.value_idx] | tonumber) end),
     (if $fields[$idx.trades_idx] == "" then 0 else ($fields[$idx.trades_idx] | tonumber) end),
     0,
     "",
     "",
     "",
     "",
     0
   ]' "${inputfile}" | \
    jq --compact-output \
      --slurp '
      # Calculate values for each sector
      def get_totals(items; sector):
        [
          "BIST",
          "",
          "sector",
          (if sector == "Borsa Istanbul" then "" else "Borsa Istanbul" end),
          "",
          "",
          sector,
          sector,
          sector,
          sector,
          sector,
          0,
          0,
          0,
          (map(.[14]) | add),
          (map(.[15]) | add),
          (map(.[16]) | add),
          (map(.[17]) | add),
          "",
          "",
          "",
          "",
          length
        ];
    {"securities": {
      "columns": [
        "exchange",
        "country",
        "type",
        "sector",
        "industry",
        "currencyId",
        "ticker",
        "nameEng",
        "nameEngShort",
        "nameOriginal",
        "nameOriginalShort",
        "priceOpen",
        "priceLastSale",
        "priceChangePct",
        "volume",
        "value",
        "numTrades",
        "marketCap",
        "listedFrom",
        "listedTill",
        "wikiPageIdEng",
        "wikiPageIdOriginal",
        "nestedItemsCount"
      ],
      "data": (
        . as $data |
        (
          # Add market total
          [get_totals($data; "Borsa Istanbul")] +
          # Add sector totals
          (group_by(.[3]) | map(get_totals(.; .[0][3]))) +
          # Original data
          $data
        )
      )
    }
  }' > "${outputfile}"
}

# Export the function
export -f create_datafiles


# Read sectors
sectors=$( jq -R 'split("\t") | { (.[1]): [.[0], .[4]] }' sectors/sectors.csv | jq -s 'add' --compact-output )


for filename in ./rawdata/*.csv; do
  # Read the datafile, skip headers
  tail -n +3 "${filename}" | \
    # Get unique dates from the datafile
    jq -R 'split(";") | .[0]' | sort -u | \
      # Create a datafile for each date (parallel processing, 4 threads)
      xargs -I '{}' -P 4 bash -c 'create_datafiles "{}" "$1" "$2"' _ "${filename}" "${sectors}"
done

