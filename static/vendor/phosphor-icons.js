var head = document.getElementsByTagName("head")[0];

for (const weight of ["regular", "thin", "light", "bold", "fill", "duotone"]) {
  var link = document.createElement("link");
  link.rel = "stylesheet";
  link.type = "text/css";
  // Cargar estilos localmente (sin dependencias a internet)
  link.href = `/static/vendor/phosphor-icons-web/src/${weight}/style.css`;
  head.appendChild(link);
}
