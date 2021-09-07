import { Component, OnInit, ViewChild, ViewEncapsulation } from "@angular/core";
import { Router } from "@angular/router";
import { FormControl, FormGroup, FormBuilder } from "@angular/forms";
import {
  StepsService,
  Step3Data,
  Step4Data,
  Step2Data
} from "../steps.service";
import { DataService } from "../../../services/data.service";
import { ContentMappingService } from "../../../services/mappings/content-mapping.service";
import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from "@geonature_common/service/cruved-store.service";
import { ModuleConfig } from "../../../module.config";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";

@Component({
  selector: "content-mapping-step",
  styleUrls: ["content-mapping-step.component.scss"],
  templateUrl: "content-mapping-step.component.html",
  encapsulation: ViewEncapsulation.None
})
export class ContentMappingStepComponent implements OnInit {
  public IMPORT_CONFIG = ModuleConfig;
  public isCollapsed = false;
  public userContentMapping;
  public newMapping: boolean = false;
  public id_mapping;
  public idFieldMapping: number;
  public columns;
  public spinner: boolean = false;
  public contentTargetForm: FormGroup;
  public showForm: boolean = false;
  public contentMapRes: any;
  public stepData: Step3Data;
  public nomencName;
  public idInfo;
  public disabled: boolean = true;
  public disableNextStep = true;
  public n_errors: number;
  public n_warnings: number;
  public n_aMapper: number = -1;
  public n_mappes: number = -1;
  public nbLignes: string = "X";
  public showValidateMappingBtn = true;
  public displayCheckBox = ModuleConfig.DISPLAY_CHECK_BOX_MAPPED_VALUES;
  public mappingListForm = new FormControl();
  public newMappingNameForm = new FormControl();

  @ViewChild("modalConfirm") modalConfirm: any;
  @ViewChild("modalRedir") modalRedir: any;

  constructor(
    private stepService: StepsService,
    private _fb: FormBuilder,
    private _ds: DataService,
    public _cm: ContentMappingService,
    private _commonService: CommonService,
    private _router: Router,
    private _modalService: NgbModal,
    public cruvedStore: CruvedStoreService
  ) { }

  ngOnInit() {
    this.stepData = this.stepService.getStepData(3);

    const step2: Step2Data = this.stepService.getStepData(2);
    this.idFieldMapping = step2.id_field_mapping;
    this.contentTargetForm = this._fb.group({});

    // show list of user mappings
    this._cm.getMappingNamesList();

    this.getNomencInf();

    // listen to change on mappingListForm select
    this.onMappingName();
  }

  getNomencInf() {
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

            this._ds.getOneBibMapping(this.stepData.id_content_mapping).subscribe(mapping => {
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

  compareFn(c1: any, c2: any): boolean {
    return c1 && c2 ? c1.id_mapping === c2.id_mapping : c1 === c2;
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

  isEnabled(value_def_id: string) {
    return true;
    /*(!this.contentTargetForm.controls[value_def_id].value)
      || this.contentTargetForm.controls[value_def_id].value.length == 0;*/
  }

  containsEnabled(contentMapping: any) {
    //return contentMapping.nomenc_values_def.find(value_def => this.isEnabled(value_def.id));
    return (
      contentMapping.user_values.values.filter(val => val.value).length > 0
    );
  }

  updateEnabled(e) {
    // this.onMappingChange(this.id_mapping);
    this._cm.displayMapped = !this._cm.displayMapped;
  }

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
  }

  onStepBack() {
    this._router.navigate([`${ModuleConfig.MODULE_URL}/process/id_import/${this.stepData.importId}/step/2`]);
  }

  onDataChecking() {
    // perform all check on file
    this.id_mapping = this.mappingListForm.value.id_mapping;
    this.spinner = true;
    this._ds
      .dataChecker(this.stepData.importId, this.idFieldMapping, this.id_mapping)
      .subscribe(
        import_obj => {
          this.spinner = false;
          //this.contentMapRes = res;
          this._ds.getErrorList(this.stepData.importId).subscribe(err => {
            this.n_errors = err.errors.filter(
              error => error.error_level == "ERROR"
            ).length;
            this.n_warnings = err.errors.filter(
              error => error.error_level == "WARNING"
            ).length;
            if (this.n_errors == 0) {
              this.disableNextStep = false;
              this.showValidateMappingBtn = false;
            }
          });
          if (import_obj.source_count < ModuleConfig.MAX_LINE_LIMIT) {
            this._router.navigate([
              `${ModuleConfig.MODULE_URL}/process/id_import/${this.stepData.importId}/step/4`
            ]);
          } else {
            this.nbLignes = import_obj.source_count;
            this._modalService.open(this.modalRedir);
          }
        },
        error => {
          this.spinner = false;
          if (error.statusText === "Unknown Error") {
            // show error message if no connexion
            this._commonService.regularToaster(
              "error",
              "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
            );
          } else {
            // show error message if other server error
            console.log(error);
            this._commonService.regularToaster("error", error.error.message);
          }
        }
      );
  }

  onRedirect() {
    this._router.navigate([ModuleConfig.MODULE_URL]);
  }

  createOrUpdateMapping(temporary) {
    this._ds
      .updateContentMapping(this.id_mapping, this.contentTargetForm.value)
      .subscribe(d => {
        let step4Data: Step4Data = {
          importId: this.stepData.importId
        };
        let step3Data: Step3Data = this.stepData;
        step3Data.id_content_mapping = this.id_mapping;
        step3Data.temporaryMapping = temporary;
        this.stepService.setStepData(3, step3Data);
        this.stepService.setStepData(4, step4Data);
        this.onDataChecking();
      });
  }

  // On close modal: ask if save the mapping or not
  saveMappingUpdate(saveBoolean) {
    if (saveBoolean) {
      this.createOrUpdateMapping(true);
    } else {
      // create a temporary mapping
      const mapping_value = {
        mappingName: "mapping_temporaire_" + Date.now(),
        temporary: true
      };
      this._ds
        .postMappingName(mapping_value, "CONTENT")
        .subscribe(id_mapping => {
          this.id_mapping = id_mapping;
          this.createOrUpdateMapping(false);
        });
    }
  }

  removeModel() {
    if (this.mappingListForm.value !== null) {
      this._ds.deleteMapping(this.id_mapping).subscribe(
        () => this._cm.getMappingNamesList()
      )
    }
  }

  goToPreview() {    
    // if mapping change
    if(!this.contentTargetForm.pristine) {
      // if the form mapping has been changed and the user has right to update it
      if (this.mappingListForm.value.cruved.U) {
        this._modalService.open(this.modalConfirm);
      } else {
        // the user don't has right to update the mapping -> temporary mapping
        const mapping_value = {
          mappingName: "mapping_temporaire_" + Date.now(),
          temporary: true
        };
        this._ds
          .postMappingName(mapping_value, "CONTENT")
          .subscribe(id_mapping => {
            this.id_mapping = id_mapping;
            this.createOrUpdateMapping(true);
          });
      }
    }
    else {
      this.onDataChecking();
    }
    
  }
}
