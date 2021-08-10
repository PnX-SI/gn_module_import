import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute } from "@angular/router";

import { DataService } from '../../services/data.service'
import { CsvExportService } from "../../services/csv-export.service";

@Component({
    selector: 'pnx-import-errors',
    templateUrl: 'import_report.component.html',
    styleUrls: ["import_report.component.scss"],

})

export class ImportReportComponent implements OnInit, OnDestroy {
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
    public doughnutChartColors: Array<Object> = [{
        backgroundColor: ['red', 'green', 'blue']
      },];
    public doughnutChartType = 'doughnut';
    private options: any = {
        legend: { position: 'left' }
      }

    constructor(private _dataService: DataService, private _csvExport: CsvExportService, private _activedRoute: ActivatedRoute) { }



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
        const grouped = this.groupBy(this.validData, "nom_cite");
        console.log(grouped);

        // Must do this otherwise the chart will not update
        this.doughnutChartLabels.length = 0;
        this.doughnutChartLabels.push(...Object.keys(grouped));
        
        // Sorting by most viewed taxons
        let data = Object.values(grouped).map(e => e.length)
        this.doughnutChartLabels.sort(function(a, b) {
            return grouped[b].length - grouped[a].length;
          });
          
        data = data.sort((a, b) => b - a);
        
        this.doughnutChartData.length = 0;
        this.doughnutChartData = data;

        // Fill colors with random colors
        let colors = new Array(this.doughnutChartData.length)
        for (let i=0; i < colors.length; i++) {
            colors[i] = "#" + ((1<<24)*Math.random() | 0).toString(16)
        } 
        this.doughnutChartColors[0].backgroundColor.push(...colors);
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