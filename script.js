function fetchData() {
    const zip = document.getElementById('zipInput').value;
    fetch('./data_output/frost_tool_dict.json') // Replace with your actual path
        .then(response => response.json())
        .then(data => {
            const info = data[zip];
            if (info) {
                const output = `
                    City: ${info.city}<br>
                    State: ${info.state}<br>
                    County: ${info.county}
                `;
                document.getElementById('output').innerHTML = output;
            } else {
                document.getElementById('output').innerHTML = "ZIP Code not found!";
            }
        })
        .catch(err => {
            document.getElementById('output').innerHTML = "Error fetching data!";
        });
}
