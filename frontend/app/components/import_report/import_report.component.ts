import { Component, OnInit } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { saveAs } from "file-saver";
import leafletImage from "leaflet-image";

import { MapService } from "@geonature_common/map/map.service";
import { ModuleConfig } from "../../module.config";
import { DataService } from "../../services/data.service";
import { CsvExportService } from "../../services/csv-export.service";
import { Step } from "../../models/enums.model";
import { ImportProcessService } from "../import_process/import-process.service";
import { Import, ImportError, ImportValues } from "../../models/import.model";
import { MappingContent, MappingField } from "../../models/mapping.model";

interface CustomNomenclature {
  nomenc_synthese_name: string;
  source_value: string;
  mnemonique: string;
  cd_nomenclature: string;
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
  public importData: Import;
  public expansionPanelHeight: string = "60px";
  public validBbox: any;
  public validData: Array<Object>;
  public fields: MappingField[];
  public taxaDistribution: Array<{ count: number; group: string }>;
  public nomenclature: ImportValues;
  public contentMapping: Array<MappingContent>;
  public matchedNomenclature: CustomNomenclature[];
  public importErrors: Array<ImportError> = null;
  public nbTotalErrors: number = 0;
  public rank: string = ""; //ModuleConfig.DEFAULT_RANK;
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
    // Load additionnal data if imported data
    this.loadValidData(this.importData.id_import);
    this.loadMapping();
    this.loadNomenclature();
    //this.loadNomenclature(this.importData.id_import, fieldMapping, contentMapping);
    //this.loadTaxaDistribution(idSource);
    // Add property to show errors lines. Need to do this to
    // show line per line...
    // data.errors.forEach((element) => {
    //   element.show = false;
    // });

    //this.rank = this.rankOptions[0]  // default
    this.loadErrors();
  }

  /** Gets the validBbox and validData (info about observations)
   * @param {string}  idImport - id of the import to get the info from
   */
  loadValidData(idImport: number) {
    this._dataService.getValidData(idImport).subscribe((data) => {
      this.validBbox = data.valid_bbox;
      this.validData = data.valid_data;
    });
  }

  loadMapping() {
    this._dataService
      .getMappingFields(this.importData.id_field_mapping)
      .subscribe((data) => {
        this.fields = data;
      });
  }

  loadNomenclature() {
    this._dataService
      .getImportValues(this.importData.id_import)
      .subscribe((data) => {
        this.nomenclature = data;
        this.loadContentMapping();
      });
  }

  loadContentMapping() {
    this._dataService
      .getMappingContents(this.importData.id_content_mapping)
      .subscribe((data) => {
        this.contentMapping = data;
        if (data) {
          this.matchNomenclature();
        }
      });
  }

  loadTaxaDistribution(idSource: number) {
    const toto = 2;
    // this._dataService.getTaxaRepartition(idSource, this.rank)
    //     .subscribe(
    //         data => {
    //             this.taxaDistribution = data
    //             this.updateChart();
    //         }
    //         )
  }

  loadErrors() {
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

  matchNomenclature() {
    // Reset this.matchedNomenclature
    this.matchedNomenclature = [];
    // For each content match the nomenclature info
    this.contentMapping.forEach((item) => {
      const mnemonique =
        this.nomenclature[item.target_field_name].nomenclature_type.mnemonique;
      const nomenclature = this.nomenclature[
        item.target_field_name
      ].nomenclatures.find((nom) => nom.id_nomenclature);
      // Temporary object of a Nomenclature
      const tempNomenc: CustomNomenclature = {
        nomenc_synthese_name: item.target_field_name,
        source_value: item.source_value,
        mnemonique: mnemonique,
        cd_nomenclature: nomenclature.cd_nomenclature,
      };
      this.matchedNomenclature.push(tempNomenc);
    });
  }

  updateChart() {
    const labels = this.taxaDistribution.map((e) => e.group);
    const data = this.taxaDistribution.map((e) => e.count);

    this.doughnutChartLabels.length = 0;
    // Must push here otherwise the chart will not update
    this.doughnutChartLabels.push(...labels);

    this.doughnutChartData.length = 0;
    this.doughnutChartData = data;

    // Fill colors with random colors
    let colors = new Array(this.doughnutChartData.length);
    for (let i = 0; i < colors.length; i++) {
      colors[i] = "#" + (((1 << 24) * Math.random()) | 0).toString(16);
    }
    this.doughnutChartColors[0].backgroundColor.push(...colors);
  }

  getChartPNG() {
    let chart = <HTMLCanvasElement>document.getElementById("chart");
    let img = document.createElement("img");
    img.src = chart.toDataURL();
    return img;
  }

  exportCorrespondances() {
    // this.fields can be null
    // 4 : tab size
    if (this.fields) {
      const blob: Blob = new Blob([JSON.stringify(this.fields, null, 4)], {
        type: "application/json",
      });
      saveAs(blob, "correspondances.json");
    }
  }

  exportNomenclatures() {
    // Exactly like the correspondances
    if (this.matchedNomenclature) {
      const blob: Blob = new Blob(
        [JSON.stringify(this.matchedNomenclature, null, 4)],
        { type: "application/json" }
      );
      saveAs(blob, "nomenclatures.json");
    }
  }

  exportAsPDF() {
    var img = document.createElement("img");
    this.loadingPdf = true;
    let chartImg = this.getChartPNG();
    leafletImage(
      this._map.map,
      function (err, canvas) {
        img.src = canvas.toDataURL("image/png");
        this._dataService
          .getPdf(this.import.id_import, img.src, chartImg.src)
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
    //this.loadTaxaDistribution(this.importData.id_source);
  }
}
