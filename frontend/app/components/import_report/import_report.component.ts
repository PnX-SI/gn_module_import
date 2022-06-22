import { Component, OnInit } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { saveAs } from "file-saver";
import leafletImage from "leaflet-image";

import { MapService } from "@geonature_common/map/map.service";
import { DataService } from "../../services/data.service";
import { CsvExportService } from "../../services/csv-export.service";
import { ImportProcessService } from "../import_process/import-process.service";
import {
  Import,
  ImportError,
  ImportValues,
  Nomenclature,
  TaxaDistribution,
} from "../../models/import.model";
import { ContentMapping, FieldMapping } from "../../models/mapping.model";

interface MatchedNomenclature {
  source: Nomenclature;
  target: Nomenclature;
}

@Component({
  selector: "pnx-import-report",
  templateUrl: "import_report.component.html",
  styleUrls: ["import_report.component.scss"],
})
export class ImportReportComponent implements OnInit {
  readonly maxErrorsLines: number = 10;
  readonly rankOptions: string[] = [
    "regne",
    "phylum",
    "classe",
    "ordre",
    "famille",
    "sous_famille",
    "tribu",
    "group1_inpn",
    "group2_inpn",
  ];
  public importData: Import | null;
  public expansionPanelHeight: string = "60px";
  public validBbox: any;
  public validData: Array<Object>;
  public fields: Array<FieldMapping> = [];
  public fieldsNb: Number = 0;
  public taxaDistribution: TaxaDistribution[];
  public nomenclature: ImportValues;
  public contentMapping: Array<ContentMapping>;
  public matchedNomenclature: MatchedNomenclature[];
  public importErrors: Array<ImportError> = [];
  public nbTotalErrors: number = 0;
  public datasetName: string = "";
  public rank: string = this.rankOptions[0]; //ModuleConfig.DEFAULT_RANK;
  public doughnutChartLabels: Array<String> = [];
  public doughnutChartData: Array<number> = [];
  public doughnutChartColors: Array<{ backgroundColor: Array<String> }> = [
    {
      backgroundColor: ["red", "green", "blue"],
    },
  ];
  public doughnutChartType: string = "doughnut";
  public options: any = {
    legend: { position: "left" },
  };
  public loadingPdf: boolean = false;

  constructor(
    private importProcessService: ImportProcessService,
    private _dataService: DataService,
    private _csvExport: CsvExportService,
    private _route: ActivatedRoute,
    private _router: Router,
    private _map: MapService
  ) {}

  ngOnInit() {
    this.importData = this.importProcessService.getImportData();
    this.fieldsNb = Object.keys(this.importData?.fieldmapping || {}).length;
    // Load additionnal data if imported data
    this.loadValidData(this.importData?.id_import);
    this.loadTaxaDistribution();
    this.loadDatasetName();
    // Add property to show errors lines. Need to do this to
    // show line per line...
    // data.errors.forEach((element) => {
    //   element.show = false;
    // });
    this.loadErrors();
    this.matchNomenclature();
  }

  /** Gets the validBbox and validData (info about observations)
   * @param {string}  idImport - id of the import to get the info from
   */
  loadValidData(idImport: number | undefined) {
    if (idImport) {
      this._dataService.getValidData(idImport).subscribe((data) => {
        this.validBbox = data.valid_bbox;
        this.validData = data.valid_data;
      });
    }
  }

  loadTaxaDistribution() {
    //FIXME get idSource from import!
    const idSource: number | undefined = this.importData?.id_source;
    if (idSource) {
      this._dataService
        .getTaxaRepartition(idSource, this.rank)
        .subscribe((data) => {
          this.taxaDistribution = data;
          this.updateChart();
        });
    }
  }

  loadDatasetName() {
    if (this.importData) {
      this._dataService
        .getDatasetFromId(this.importData.id_dataset)
        .subscribe((data) => {
          this.datasetName = data.dataset_name;
        });
    }
  }

  loadErrors() {
    if (this.importData) {
      this._dataService
        .getImportErrors(this.importData.id_import)
        .subscribe((errors) => {
          this.importErrors = errors;
          // Get the total number of errors:
          // 1. get all rows in errors
          // 2. flaten to have 1 array of all rows in error
          // 3. remove duplicates (with Set)
          // 4. return the size of the Set (length)
          this.nbTotalErrors = new Set(
            errors
              .map((item) => item.rows)
              .reduce((acc, val) => acc.concat(val), [])
          ).size;
        });
    }
  }

  matchNomenclature() {
    if (this.importData) {
      const mappingValues = this.importData?.contentmapping;
      const match: MatchedNomenclature[] = [];
      this._dataService
        .getImportValues(this.importData.id_import)
        .subscribe((importValues) => {
          Object.keys(importValues).forEach((targetField) => {
            let type_mnemo =
              importValues[targetField].nomenclature_type.mnemonique;
            importValues[targetField].values.forEach((value, index) => {
              let sourceNomenclature = importValues[
                targetField
              ].nomenclatures.find(
                (n) => n.cd_nomenclature === mappingValues[type_mnemo][value]
              );
              let targetNomenclature = importValues[
                targetField
              ].nomenclatures.find((n) => n.label_default == value);
              match.push({
                source: sourceNomenclature,
                target: targetNomenclature,
              });
            });
          });
        });
      this.matchedNomenclature = match;
    }
  }

  updateChart() {
    const labels: string[] = this.taxaDistribution.map((e) => e.group);
    const data: number[] = this.taxaDistribution.map((e) => e.count);

    this.doughnutChartLabels.length = 0;
    // Must push here otherwise the chart will not update
    this.doughnutChartLabels.push(...labels);

    this.doughnutChartData.length = 0;
    this.doughnutChartData = data;

    // Fill colors with random colors
    const colors: string[] = new Array(this.doughnutChartData.length);
    for (let i = 0; i < colors.length; i++) {
      colors[i] = "#" + (((1 << 24) * Math.random()) | 0).toString(16);
    }
    this.doughnutChartColors[0].backgroundColor.push(...colors);
  }

  getChartPNG(): HTMLImageElement {
    const chart: HTMLCanvasElement = <HTMLCanvasElement>(
      document.getElementById("chart")
    );
    const img: HTMLImageElement = document.createElement("img");
    img.src = chart.toDataURL();
    return img;
  }

  exportCorrespondances() {
    // this.fields can be null
    // 4 : tab size
    if (this.importData?.fieldmapping) {
      const blob: Blob = new Blob(
        [JSON.stringify(this.importData.fieldmapping, null, 4)],
        {
          type: "application/json",
        }
      );
      saveAs(blob, "correspondances.json");
    }
  }

  exportNomenclatures() {
    // Exactly like the correspondances
    if (this.matchedNomenclature) {
      const blob: Blob = new Blob(
        [JSON.stringify(this.matchedNomenclature, null, 4)],
        {
          type: "application/json",
        }
      );
      saveAs(blob, "nomenclatures.json");
    }
  }

  exportAsPDF() {
    const img: HTMLImageElement = document.createElement("img");
    this.loadingPdf = true;
    const chartImg: HTMLImageElement = this.getChartPNG();
    leafletImage(
      this._map.map,
      function (err, canvas) {
        img.src = canvas.toDataURL("image/png");
        this._dataService
          .getPdf(this.importData.id_import, img.src, chartImg.src)
          .subscribe(
            (result) => {
              this.loadingPdf = false;
              saveAs(result, "export.pdf");
            },
            (error) => {
              this.loadingPdf = false;
              console.log("Error getting pdf");
            }
          );
      }.bind(this)
    );
  }

  goToSynthese(idDataSet: number) {
    let navigationExtras = {
      queryParams: {
        id_dataset: idDataSet,
      },
    };
    this._router.navigate(["/synthese"], navigationExtras);
  }

  onRankChange($event) {
    this.loadTaxaDistribution();
  }
}
