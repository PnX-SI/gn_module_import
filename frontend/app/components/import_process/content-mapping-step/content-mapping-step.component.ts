import { Component, OnInit, ViewChild, ViewEncapsulation } from "@angular/core";
import { Router, ActivatedRoute } from "@angular/router";
import { Validators } from "@angular/forms";
import { FormControl, FormGroup, FormBuilder } from "@angular/forms";
import { HttpErrorResponse } from "@angular/common/http";

import { Observable, of } from "rxjs";
import { forkJoin } from "rxjs/observable/forkJoin";
import { startWith, pairwise, concatMap, mapTo, finalize, tap } from "rxjs/operators";

import { DataService } from "../../../services/data.service";
import { ContentMappingService } from "../../../services/mappings/content-mapping.service";
import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from "@geonature_common/service/cruved-store.service";
import { ModuleConfig } from "../../../module.config";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { Mapping, MappingContent } from "../../../models/mapping.model";
import { Step } from "../../../models/enums.model";
import { Import, ImportValues, Nomenclature } from "../../../models/import.model";
import { ImportProcessService } from "../import-process.service";


@Component({
  selector: "content-mapping-step",
  styleUrls: ["content-mapping-step.component.scss"],
  templateUrl: "content-mapping-step.component.html",
  encapsulation: ViewEncapsulation.None
})
export class ContentMappingStepComponent implements OnInit {
  public step: Step;
  public selectMappingContentForm = new FormControl();
  public importData: Import;
  public userContentMappings: Array<Mapping>;
  public importValues: ImportValues;
  public showForm: boolean = false;
  public contentTargetForm: FormGroup;
  public spinner: boolean = false;
  public mappedFields: Set<string>;  // FIXME
  public unmappedFields: Set<string>; // FIXME

  @ViewChild("modalConfirm") modalConfirm: any;
  @ViewChild("modalRedir") modalRedir: any;

  constructor(
    //private stepService: StepsService,
    private _fb: FormBuilder,
    private _ds: DataService,
    public _cm: ContentMappingService,
    private _commonService: CommonService,
    private _router: Router,
    private _route: ActivatedRoute,
    private _modalService: NgbModal,
    public cruvedStore: CruvedStoreService,
    private importProcessService: ImportProcessService,
  ) { }

  ngOnInit() {
    this.step = this._route.snapshot.data.step;
    this.importData = this.importProcessService.getImportData();
    this.contentTargetForm = this._fb.group({});

    forkJoin({
        contentMappings: this._ds.getMappings('content'),
        importValues: this._ds.getImportValues(this.importData.id_import),
    }).subscribe(({contentMappings, importValues}) => {
        this.userContentMappings = contentMappings;

        this.importValues = importValues;
        this.contentTargetForm = this._fb.group({});
        for (let targetField of Object.keys(this.importValues)) {
            this.importValues[targetField].nomenclature_type.isCollapsed = false;
            this.importValues[targetField].values.forEach((value, index) => {
                let control = new FormControl(null, [Validators.required]);
                let control_name = targetField + '-' + index;
                this.contentTargetForm.addControl(control_name, control);
                // Search for a nomenclature with a label equals to the user value.
                for (let nomenclature of this.importValues[targetField].nomenclatures) {
                    if (value == nomenclature.label_default) {
                        control.setValue(nomenclature);
                        break;
                    }
                }
                // TODO: if value is null, select the default nomenclature!
                // require to add default nomenclature in data returned by the API
            });
        }
        this.showForm = true;
    });
  }

   // Used by select component to compare content mappings
  areMappingContentEqual(mc1: MappingContent, mc2: MappingContent): boolean {
    return (mc1 == null && mc2 == null) || (mc1 != null && mc2 != null && mc1.id_mapping === mc2.id_mapping);
  }

  areNomenclaturesEqual(n1: Nomenclature, n2: Nomenclature): boolean {
    return (n1 == null && n2 == null) || (n1 != null && n2 != null && n1.cd_nomenclature === n2.cd_nomenclature);
  }

  onSelectNomenclature(targetFieldValue: string) {
    let formControl = this.contentTargetForm.controls[targetFieldValue];
  }

  onPreviousStep() {
    this.importProcessService.navigateToPreviousStep(this.step);
  }

  isNextStepAvailable(): boolean {
    return this.contentTargetForm.valid;
  }

  onNextStep() {
    if (!this.contentTargetForm.pristine) {
      // TODO: check cruved before updating mapping
      // this.selectMappingContentForm.value.id_mapping
      /*console.log("create mapping");
      this._ds.createMapping('', 'content').subscribe(mapping => {
        console.log("mapping created", mapping.id_mapping);
        this.updateMappingContents(mapping.id_mapping).subscribe(mappingvalues => {
          console.log("mapping updated, set import mapping");
          this._ds.setImportContentMapping(this.importData.id_import, mapping.id_mapping).subscribe(importData => {
            console.log("import updated");
            this.importProcessService.setImportData(importData);
            this.importProcessService.navigateToLastStep();
          });
        });
      });*/
      this.submit(false);
    } else {
      this.importProcessService.navigateToNextStep(this.step);
    }
  }

  updateMappingContents(id_mapping: number): Observable<Array<MappingContent>> {
    let data = []
    for (let targetField of Object.keys(this.importValues)) {
        this.importValues[targetField].values.forEach((value, index) => {
            let control = this.contentTargetForm.controls[targetField + '-' + index];
            data.push({'target_field_name': targetField, 'source_value': value, 'target_id_nomenclature': control.value.id_nomenclature});
        });
    }
    return this._ds.updateMappingContents(id_mapping, data);
  }

  submit(save: boolean, mapping_name: string = '') {
    console.log("save", save, "mapping_name", mapping_name);
    this.spinner = true;
    of(1).pipe(
      concatMap(() => {
        if (save) { // the mapping must be saved (non temporarly)
          if (mapping_name) { // create a new mapping with the given name
            console.log("create mapping " + mapping_name);
            return this._ds.createMapping(mapping_name, 'content');
          } else { // update the currently selected mapping
            return of(this.selectMappingContentForm.value);
          }
        } else { // create a temporarly mapping
          console.log("create temporary mapping ");
          return this._ds.createMapping('', 'content');
        }
      }),
      concatMap((mapping: Mapping) => {
        console.log("mapping created (eventually) (id=" + mapping.id_mapping + "), updating mapping");
        return this.updateMappingContents(mapping.id_mapping).pipe(mapTo(mapping));
      }),
      concatMap((mapping: Mapping) => {
        console.log("mapping fields updated");
        if (this.importData.id_content_mapping != mapping.id_mapping) {
          console.log("set import content mapping");
          return this._ds.setImportContentMapping(this.importData.id_import, mapping.id_mapping);
        } else {
          console.log("import field mapping not changed, nothing to do");
          return of(this.importData);
        }
      }),
      concatMap((importData: Import) => {
        console.log("prepare import"); // TODO: skip if uneeded (mapping not changed & unmodified)
        return this._ds.prepareImport(importData.id_import);
      }),
      finalize(() => this.spinner = false),
    ).subscribe((importData: Import) => {
      this.importProcessService.setImportData(importData);
      this.importProcessService.navigateToLastStep();
    }, (error: HttpErrorResponse) => {
      this._commonService.regularToaster('error', error.error.description);
    });
  }


  /*getNomencInf() {
    this._ds
      .getNomencInfo(this.stepData.importId, this.idFieldMapping)
      .subscribe(
        res => {
          res["content_mapping_info"].forEach(user_col => {
            user_col.user_values.values = user_col.user_values.values.filter(val => val.value !== null)
          });
          this.stepData.contentMappingInfo = res["content_mapping_info"];

          this.generateContentForm();
          // fill the form
          if (this.stepData.id_content_mapping) {

            this._ds.getMapping(this.stepData.id_content_mapping).subscribe(mapping => {
              let mapping_already_there = false;
              this._cm.userContentMappings.forEach(curMapping => {
                if (curMapping.id_mapping == this.stepData.id_content_mapping) {
                  mapping_already_there = true;
                }
              });
              if (!mapping_already_there) {
                this._cm.userContentMappings.push(mapping)
              }
              this.mappingListForm.setValue(mapping);
              this.fillMapping(this.stepData.id_content_mapping);

            })

          }
        },
        error => {
          if (error.statusText === "Unknown Error") {
            // show error message if no connexion
            this._commonService.regularToaster(
              "error",
              "Une erreur s'est produite : contactez l'administrateur du site"
            );
          } else {
            // show error message if other server error
            console.log(error);
            this._commonService.regularToaster("error", error.error.message);
          }
        }
      );
  }

  saveMappingName(value) {
    // save new mapping in bib_mapping
    // then select the mapping name in the select
    let mappingType = "CONTENT";
    const mappingForm = {
      mappingName: this.newMappingNameForm.value
    };
    this._ds.postMappingName(mappingForm, mappingType).subscribe(
      id_mapping => {
        this._cm.newMapping = false;
        this._cm.getMappingNamesList(id_mapping, this.mappingListForm);
        this.newMappingNameForm.reset();
        //this.enableMapping(targetForm);
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "Une erreur s'est produite : contactez l'administrateur du site"
          );
        } else {
          console.log(error);
          this._commonService.regularToaster("error", error.error);
        }
      }
    );
  }

  generateContentForm() {
    this.n_aMapper = 0;
    this.stepData.contentMappingInfo.forEach(ele => {
      ele["nomenc_values_def"].forEach(nomenc => {
        this.contentTargetForm.addControl(nomenc.id, new FormControl(""));
        ++this.n_aMapper;
      });
    });
    this.showForm = true;
  }

  onSelectChange(selectedVal, group, formControlName) {
    this.stepData.contentMappingInfo.map(ele => {
      if (ele.nomenc_abbr === group.nomenc_abbr) {
        ele.user_values.values = ele.user_values.values.filter(value => {
          return !(value.value == selectedVal.value);
        });
      }
    });
  }

  onSelectDelete(deletedVal, group, formControlName) {
    this.stepData.contentMappingInfo.map(ele => {
      if (ele.nomenc_abbr === group.nomenc_abbr) {
        let temp_array = ele.user_values.values;
        temp_array.push(deletedVal);
        ele.user_values.values = temp_array.slice(0);
      }
    });

    // modify contentTargetForm control values
    let values = this.contentTargetForm.controls[formControlName].value;
    values = values.filter(value => {
      return value.id != deletedVal.id;
    });
    this.contentTargetForm.controls[formControlName].setValue(values);
  }

  //isEnabled(value_def_id: string) {
  //  return true;
  //  //(!this.contentTargetForm.controls[value_def_id].value)
  //  //  || this.contentTargetForm.controls[value_def_id].value.length == 0;
  //}

  //containsEnabled(contentMapping: any) {
  //  //return contentMapping.nomenc_values_def.find(value_def => this.isEnabled(value_def.id));
  //  return (
  //    contentMapping.user_values.values.filter(val => val.value).length > 0
  //  );
  //}

  //updateEnabled(e) {
  //  // this.onMappingChange(this.id_mapping);
  //  this._cm.displayMapped = !this._cm.displayMapped;
  //}

  onMappingName(): void {
    this.mappingListForm.valueChanges.subscribe(mapping => {

      if (mapping && mapping.id_mapping) {
        this.disabled = false;
        this.fillMapping(mapping.id_mapping);
      } else {
        this.n_mappes = -1;
        this.contentTargetForm.reset();
        for (let contentMapping of this.stepData.contentMappingInfo) {
          contentMapping.isCollapsed = false;
        }
        this.disabled = true;
      }
    });
  }

  getId(userValue, nomencId) {
    this.stepData.contentMappingInfo.forEach(contentMapping => {
      // find nomenc
      contentMapping.nomenc_values_def.forEach(ele => {
        if (ele.id == nomencId) {
          this.nomencName = contentMapping.nomenc_abbr;
        }
      });
    });

    return this.idInfo;
  }

  fillMapping(id_mapping) {
    this.id_mapping = id_mapping;
    this._ds.getMappingContents(id_mapping).subscribe(mappingContents => {
      this.contentTargetForm.reset();
      if (mappingContents[0] != "empty") {

        this.n_mappes = 0;
        for (let content of mappingContents) {
          let arrayVal: any = [];

          for (let val of content) {
            if (val["source_value"] != "") {
              let id_info = this.getId(
                val["source_value"],
                val["id_target_value"]
              );
              arrayVal.push({ id: id_info, value: val["source_value"] });
            }
          }
          const formControl = this.contentTargetForm.get(
            String(content[0]["id_target_value"])
          );

          if (formControl) {
            formControl.setValue(arrayVal)
            this.n_mappes = this.n_mappes + 1;
          }

        }
      } else {
        this.contentTargetForm.reset();
        this.n_mappes = -1;
      }
      this.n_aMapper = 0;
      for (let contentMapping of this.stepData.contentMappingInfo) {
        this.n_aMapper += contentMapping.user_values.values.filter(
          val => val.value
        ).length;
        this.n_mappes -= contentMapping.user_values.values.filter(
          val => val.value
        ).length;
      }
      // at the end set the formgroup as pristine
      this.contentTargetForm.markAsPristine();
    }),
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          this._commonService.regularToaster("error", error.error.message);
        }
      };
  }*/
}
