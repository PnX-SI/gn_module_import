import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from "@angular/router";
import { saveAs } from "file-saver";

import { DataService } from '../../services/data.service'
import { CsvExportService } from "../../services/csv-export.service";

@Component({
    selector: 'pnx-import-errors',
    templateUrl: 'import_report.component.html',
    styleUrls: ["import_report.component.scss"],

})

export class ImportReportComponent implements OnInit, OnDestroy {
    readonly maxTaxa: number = 10;
    readonly maxErrorsLines: number = 10;

    private sub: any;
    public import: any;
    public formatedErrors: string;
    public expansionPanelHeight: string = "60px";
    public validBbox: Object;
    public validData: Array<Object>;
    public fields: Array<Object>;
    public nomenclature: Array<Object>;
    public doughnutChartLabels: Array<String> = [];
    public doughnutChartData: Array<number> = [];
    public doughnutChartColors: Array<{ backgroundColor: Array<String>}> = [{
        backgroundColor: ['red', 'green', 'blue']
      }];
    public doughnutChartType = 'doughnut';
    private options: any = {
        legend: { position: 'left' }
      }

    constructor(
        private _dataService: DataService, 
        private _csvExport: CsvExportService, 
        private _activedRoute: ActivatedRoute,
        private _router: Router) { }

    ngOnInit() {

        this.sub = this._activedRoute.params.subscribe(params => {
            const idImport: number = params["id_import"];
            this._dataService.getOneImport(idImport).subscribe(data => {
                if (data.import_count) {
                    // Load additionnal data if imported data
                    this.loadValidData(idImport);
                    const fieldMapping = data.id_field_mapping;
                    this.loadMapping(fieldMapping);
                    this.loadNomenclature(idImport, fieldMapping);
                }
                // Add property to show errors lines. Need to do this to
                // show line per line...
                data.errors.forEach(element => {
                    element.show = false
                });

                this.import = data;
            })
        })
    }

    /** Gets the validBbox and validData (info about observations)
     * @param {string}  idImport - id of the import to get the info from
     */
    loadValidData(idImport: number) {
        this._dataService.getValidData(idImport
        ).subscribe(data => {
            this.validBbox = data.valid_bbox;
            this.validData = data.valid_data;
            this.updateChart();
        })
    }

    loadMapping(idMapping: number) {
        this._dataService.getMappingFields(idMapping)
            .subscribe(data => {
                this.fields = data
            })
    }

    loadNomenclature(idImport: number, idMapping: number) {
        this._dataService.getNomencInfo(idImport, idMapping)
            .subscribe(data => {
                console.log(data)
                this.nomenclature = data
            })
    }

    updateChart() {
        // Chart:
        const grouped: Object = this.groupBy(this.validData, "nom_cite");

        let labels = Object.keys(grouped);
        
        // Sorting by most viewed taxons
        let data = Object.values(grouped).map(e => e.length)
        labels.sort(function(a, b) {
            return grouped[b].length - grouped[a].length;
          });
        
        data = data.sort((a, b) => b - a);
        
        this.doughnutChartLabels.length = 0;
        // Must push here otherwise the chart will not update
        this.doughnutChartLabels.push(...labels.slice(0, this.maxTaxa));

        this.doughnutChartData.length = 0;
        this.doughnutChartData = data.slice(0, this.maxTaxa);

        // Fill colors with random colors
        let colors = new Array(this.doughnutChartData.length)
        for (let i=0; i < colors.length; i++) {
            colors[i] = "#" + ((1<<24)*Math.random() | 0).toString(16)
        }
        this.doughnutChartColors[0].backgroundColor.push(...colors);
    }

    exportCorrespondances() {
        // this.fields can be null
        if (this.fields) {
            const blob = new Blob([JSON.stringify(this.fields, null, 4)], 
                                {type : 'application/json'});
            saveAs(blob, "correspondances.json");
        }
    }

    goToSynthese(idDataSet: number) {
        let navigationExtras = {
              queryParams: {
                "id_dataset": idDataSet
              }
        };
        this._router.navigate(['/synthese'], navigationExtras);
    }

    groupBy(xs, key) {
        return xs.reduce(function(rv, x) {
          (rv[x[key]] = rv[x[key]] || []).push(x);
          return rv;
        }, {});
      };

    ngOnDestroy() {
        this.sub.unsubscribe();
    }
}