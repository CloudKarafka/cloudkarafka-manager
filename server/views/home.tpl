<h1> CloudKarafka's Apache Kafka Management Interface </h1>

<div>
  <h2> Brokers </h2>
  <table>
    <thead>
      <tr>
        <th> Id </th>
        <th> Version </th>
        <th> Advertised Ports </th>
        <th> Uptime </th>
      </tr>
    </thead>
    <tbody>
      {{ range .Brokers }}
        <tr>
          <td> {{.Id }} </td>
          <td> {{.Version }} </td>
          <td> {{.AdvertisedPorts }} </td>
          <td> {{.Uptime }} </td>
        </tr>
      {{ end}}
    </tbody>
  </table>
</div>
<div>
  <h2> Topics </h2>
  <table>
    <thead>
      <tr>
        <th> Name </th>
        <th> Partitions </th>
        <th> Replicas </th>
        <th> Config </th>
      </tr>
    </thead>
    <tbody>
      {{ range .Topics }}
        <tr>
          <td> {{.Name }} </td>
          <td> {{.Partitions }} </td>
          <td> {{.Replicas }} </td>
          <td> {{.Config }} </td>
        </tr>
      {{ end}}
    </tbody>
  </table>
</div>

