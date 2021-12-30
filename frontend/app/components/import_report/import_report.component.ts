import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from "@angular/router";
import { saveAs } from "file-saver";
import  leafletImage from 'leaflet-image';

import { MapService } from '@geonature_common/map/map.service';
import { ModuleConfig } from "../../module.config";
import { DataService } from '../../services/data.service';
import { CsvExportService } from "../../services/csv-export.service";
import FixModel from '../need-fix/need-fix.models';

@Component({
    selector: 'pnx-import-errors',
    templateUrl: 'import_report.component.html',
    styleUrls: ["import_report.component.scss"],

})

export class ImportReportComponent implements OnInit, OnDestroy {
    readonly maxErrorsLines: number = 10;
    readonly rankOptions: string[] = ['regne', 
                                      'phylum',
                                      'classe',
                                      'ordre',
                                      'famille',
                                      'sous_famille',
                                      'tribu',
                                      'group1_inpn',
                                      'group2_inpn']
    private sub: any;
    public import: any;
    public formatedErrors: string;
    public expansionPanelHeight: string = "60px";
    public validBbox: Object;
    public validData: Array<Object>;
    public fix: FixModel;
    public fields: Array<Object>;
    public taxaDistribution: Array<{ count: number, group: string }>;
    public nomenclature: any;
    public contentMapping: any;
    public matchedNomenclature: any;
    public rank: string = ModuleConfig.DEFAULT_RANK;
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
                this.loadValidData(idImport);
                const fieldMapping = data.id_field_mapping;
                const contentMapping = data.id_content_mapping;
                this.loadMapping(fieldMapping);
                this.loadNomenclature(idImport, fieldMapping, contentMapping);
                if (data.import_count) {
                    const idSource = data.id_source;
                    // Load additionnal data if imported data
                    this.loadTaxaDistribution(idSource)
                }
                // Add property to show errors lines. Need to do this to
                // show line per line...
                data.errors.forEach(element => {
                    element.show = false
                });

                this.import = data;
            })
        })

        //this.rank = this.rankOptions[0]  // default
    }

    /** Gets the validBbox and validData (info about observations)
     * @param {string}  idImport - id of the import to get the info from
     */
    loadValidData(idImport: number) {
        this._dataService.getValidData(idImport
        ).subscribe(data => {
            this.validBbox = data.valid_bbox;
            this.validData = data.valid_data;
            this.fix = data.fix;
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

    loadTaxaDistribution(idSource) {
        this._dataService.getTaxaRepartition(idSource, this.rank)
            .subscribe(
                data => {
                    this.taxaDistribution = data
                    this.updateChart();
                }
                )

    }

    matchNomenclature() {
        this.matchedNomenclature = []
        let sourceValues = this.nomenclature.content_mapping_info.map(
            elm => elm.nomenc_values_def).flat();
        
        if (sourceValues.length > 0) {
            this.matchedNomenclature = this.contentMapping.map(elm => elm[0])
            
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
    }

    updateChart() {
        const labels = this.taxaDistribution.map(e => e.group)
        const data = this.taxaDistribution.map(e => e.count)
        
        this.doughnutChartLabels.length = 0;
        // Must push here otherwise the chart will not update
        this.doughnutChartLabels.push(...labels);

        this.doughnutChartData.length = 0;
        this.doughnutChartData = data;

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
        // 4 : tab size
        if (this.fields) {
            const blob = new Blob([JSON.stringify(this.fields, null, 4)], 
                                {type : 'application/json'});
            saveAs(blob, "correspondances.json");
        }
    }

    exportNomenclatures() {
        // Exactly like the correspondances
        if (this.matchedNomenclature) {
            const blob = new Blob([JSON.stringify(this.matchedNomenclature, null, 4)], 
                                {type : 'application/json'});
            saveAs(blob, "nomenclatures.json");
        }
    }
    
    exportAsPDF() {
        var img = document.createElement('img');
        this.loadingPdf = true
        // Init chartImg
        let chartImg = document.createElement('img');
        // Check if the chart exists
        if  (<HTMLCanvasElement>document.getElementById('chart')) {
            let chartImg = this.getChartPNG();
        }
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

    onRankChange($event) {
        this.loadTaxaDistribution(this.import.id_source)
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