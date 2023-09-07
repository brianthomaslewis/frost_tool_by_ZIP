let data = [
    {"zipcode":"00601","state_province":"PR","country":"RQ","station_name":"ADJUNTAS SUBSTN","station_altitude":1830,"station_distance_miles":2.6,"last_freeze":"infrequent frost","first_freeze":"infrequent frost","growing_days":365},
    {"zipcode":"00602","state_province":"PR","country":"RQ","station_name":"COLOSO","station_altitude":40,"station_distance_miles":1.7,"last_freeze":"infrequent frost","first_freeze":"infrequent frost","growing_days":365}
];

function fetchData() {
    let input = document.getElementById('zipcode-input').value;
    let resultDiv = document.getElementById('results'); 
    let result = data.find(d => d.zipcode === input);

    if (result) {
        resultDiv.innerHTML = `
        <h3>Details for ZIP Code: ${result.zipcode}</h3>
        <p><strong>State/Province:</strong> ${result.state_province}</p>
        <p><strong>Country:</strong> ${result.country}</p>
        <p><strong>Station Name:</strong> ${result.station_name}</p>
        <p><strong>Station Altitude:</strong> ${result.station_altitude} ft.</p>
        <p><strong>Station Distance:</strong> ${result.station_distance_miles} miles</p>
        <p><strong>Last Freeze:</strong> ${result.last_freeze}</p>
        <p><strong>First Freeze:</strong> ${result.first_freeze}</p>
        <p><strong>Growing Days:</strong> ${result.growing_days}</p>
        `;
    } else {
        resultDiv.innerHTML = `<p>No data found for ZIP Code: ${input}</p>`;
    }
}
