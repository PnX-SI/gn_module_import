import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from "@angular/router";
import { saveAs } from "file-saver";
import  leafletImage from 'leaflet-image';

import { MapService } from '@geonature_common/map/map.service';
import { DataService } from '../../services/data.service';
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
    public nomenclature: any;
    public contentMapping: any;
    public matchedNomenclature: any;

    public doughnutChartLabels: Array<String> = [];
    public doughnutChartData: Array<number> = [];
    public doughnutChartColors: Array<{ backgroundColor: Array<String>}> = [{
        backgroundColor: ['red', 'green', 'blue']
      }];
    public doughnutChartType = 'doughnut';
    private options: any = {
        legend: { position: 'left' }
      };
    public loadingPdf = false 

    constructor(
        private _dataService: DataService, 
        private _csvExport: CsvExportService, 
        private _activedRoute: ActivatedRoute,
        private _router: Router,
        private _map: MapService) { }

    ngOnInit() {

        this.sub = this._activedRoute.params.subscribe(params => {
            const idImport: number = params["id_import"];
            this._dataService.getOneImport(idImport).subscribe(data => {
                if (data.import_count) {
                    // Load additionnal data if imported data
                    this.loadValidData(idImport);
                    const fieldMapping = data.id_field_mapping;
                    const contentMapping = data.id_content_mapping;
                    this.loadMapping(fieldMapping);
                    this.loadNomenclature(idImport, fieldMapping, contentMapping);
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

    loadNomenclature(idImport: number, idMapping: number, idContentMapping: number) {
        this._dataService.getNomencInfo(idImport, idMapping)
            .subscribe(data => {
                this.nomenclature = data
                this.loadContentMapping(idContentMapping);
            })
    }

    loadContentMapping(idContentMapping: number) {
        this._dataService.getMappingContents(idContentMapping)
            .subscribe(data => {
                this.contentMapping = data
                if (!data.includes("empty")) { 
                    this.matchNomenclature()
                }
            })
    }

    matchNomenclature() {
        this.matchedNomenclature = this.contentMapping.map(elm => elm[0])
        let sourceValues = this.nomenclature.content_mapping_info.map(
            elm => elm.nomenc_values_def).flat();
        // For each values in target_values (this.matchedNomenclature)
        // filter with the id of target_values with the id of source
        // values to get the actual value
        // Then affect the target_value and the definition
        this.matchedNomenclature.forEach(
            val => sourceValues.filter(
                elm => parseInt(elm.id) == val.id_target_value).map(
                    function(elm) {
                        // Carefull the target_value is actually the
                        // source value, needs to rename
                        val.target_value = val.source_value
                        val.source_value = elm.value;
                        val.definition = elm.definition}
                        )[0]
                )
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

    getChartPNG() {
        let chart = <HTMLCanvasElement>document.getElementById('chart')
        let img = document.createElement('img');
        img.src = chart.toDataURL()
        return img
    }

    exportCorrespondances() {
        // this.fields can be null
        if (this.fields) {
            const blob = new Blob([JSON.stringify(this.fields, null, 4)], 
                                {type : 'application/json'});
            saveAs(blob, "correspondances.json");
        }
    }
    
    exportAsPDF() {
        var img = document.createElement('img');
        this.loadingPdf = true
        let chartImg = this.getChartPNG()
        leafletImage(this._map.map, function(err, canvas) {
            img.src = canvas.toDataURL('image/png');
            this._dataService.getPdf(this.import.id_import, img.src, chartImg.src)
                .subscribe(
                    result => {
                        this.loadingPdf = false;
                        saveAs(result, 'export.pdf');
                    },
                    error => {
                        this.loadingPdf = false;
                        console.log('Error getting pdf');
                    }
                );
        }.bind(this));
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