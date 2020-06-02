import { Component, OnInit, ViewChild } from "@angular/core";
import { Router } from "@angular/router";
import { FormControl, FormGroup, FormBuilder } from "@angular/forms";
import { StepsService, Step3Data, Step4Data, Step2Data } from "../steps.service";
import { DataService } from "../../../services/data.service";
import { ContentMappingService } from "../../../services/mappings/content-mapping.service";
import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from "@geonature_common/service/cruved-store.service";
import { ModuleConfig } from "../../../module.config";
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: "content-mapping-step",
  styleUrls: ["content-mapping-step.component.scss"],
  templateUrl: "content-mapping-step.component.html"
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
  contentTargetForm: FormGroup;
  showForm: boolean = false;
  contentMapRes: any;
  stepData: Step3Data;
  public nomencName;
  public idInfo;
  public disabled: boolean = true;
  public disableNextStep = true;
  public n_errors: number;
  public n_warnings: number;
  public n_aMapper: number = -1;
  public n_mappes: number = -1;
  public showValidateMappingBtn = true;
  public displayCheckBox = ModuleConfig.DISPLAY_CHECK_BOX_MAPPED_VALUES;
  public mappingListForm = new FormControl();
  public newMappingNameForm = new FormControl();

  @ViewChild('modalConfirm') modalConfirm: any;

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


    // fill the form
    if (this.stepData.id_content_mapping) {
      const formValue = {
        "id_mapping": this.stepData.id_content_mapping,
        "cruved": this.stepData.cruvedMapping
      }
      this.mappingListForm.setValue(
        formValue
      );
      this.fillMapping(this.stepData.id_content_mapping);

    }
  }

  getNomencInf() {
    this._ds.getNomencInfo(this.stepData.importId, this.idFieldMapping).subscribe(
      res => {
        this.stepData.contentMappingInfo = res["content_mapping_info"];
        this.generateContentForm();
      },
      error => {
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

  saveMappingName(value) {
    // save new mapping in bib_mapping
    // then select the mapping name in the select
    let mappingType = "CONTENT";
    const mappingForm = {
      'mappingName': this.newMappingNameForm.value
    }
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
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
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
        /*if (ele.nomenc_abbr == 'NAT_OBJ_GEO') {
          console.log(ele.user_values.values);
          console.log(selectedVal);
        }*/
        ele.user_values.values = ele.user_values.values.filter(value => {
          return !((value.value == selectedVal.value));
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
    return contentMapping.user_values.values.filter(val => val.value).length > 0;
  }

  updateEnabled(e) {
    // this.onMappingChange(this.id_mapping);
    this._cm.displayMapped = !this._cm.displayMapped;
  }

  onMappingName(): void {
    this.mappingListForm.valueChanges.subscribe(
      mapping => {
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
        } // onMappingName(): void {
        //   this.contentMappingForm.get("contentMapping").valueChanges.subscribe(
        //     id_mapping => {
        //       //this.onMappingChange(id_mapping);
        //     },
        //     error => {
        //       if (error.statusText === "Unknown Error") {
        //         // show error message if no connexion
        //         this._commonService.regularToaster(
        //           "error",
        //           "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
        //         );
        //       } else {
        //         console.log(error);
        //         this._commonService.regularToaster("error", error.error);
        //       }
        //     }
        //   );
        // }

      }
    );
  }

  // onMappingName(): void {
  //   this.contentMappingForm.get("contentMapping").valueChanges.subscribe(
  //     id_mapping => {
  //       //this.onMappingChange(id_mapping);
  //     },
  //     error => {
  //       if (error.statusText === "Unknown Error") {
  //         // show error message if no connexion
  //         this._commonService.regularToaster(
  //           "error",
  //           "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
  //         );
  //       } else {
  //         console.log(error);
  //         this._commonService.regularToaster("error", error.error);
  //       }
  //     }
  //   );
  // }

  getId(userValue, nomencId) {
    this.stepData.contentMappingInfo.forEach(contentMapping => {
      // find nomenc
      contentMapping.nomenc_values_def.forEach(ele => {
        if (ele.id == nomencId) {
          this.nomencName = contentMapping.nomenc_abbr;
        }
      });
    });
    // console.log(this.idInfo);

    return this.idInfo;
  }

  fillMapping(id_mapping) {
    this.id_mapping = id_mapping;
    this._ds.getMappingContents(id_mapping).subscribe(
      mappingContents => {
        // console.log(mappingContents);
        this.contentTargetForm.reset();
        if (mappingContents[0] != "empty") {
          this.n_mappes = 0;
          for (let content of mappingContents) {
            let arrayVal: any = [];
            // console.log(content);

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
              formControl.setValue(arrayVal);
              ++this.n_mappes;
            }
          }
        } else {
          this.contentTargetForm.reset();
          this.n_mappes = -1;
        }
        this.n_aMapper = 0;
        for (let contentMapping of this.stepData.contentMappingInfo) {
          this.n_aMapper += contentMapping.user_values.values.filter(val => val.value).length;
          this.n_mappes -= contentMapping.user_values.values.filter(val => val.value).length;
          if (contentMapping.user_values.values.filter(val => val.value).length > 0)
            console.log(contentMapping.user_values);
        }
        // at the end set the formgroup as pristine
        this.contentTargetForm.markAsPristine();
      },
    ), error => {
      if (error.statusText === "Unknown Error") {
        // show error message if no connexion
        this._commonService.regularToaster(
          "error",
          "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
        );
      } else {
        this._commonService.regularToaster("error", error.error.message);
      }
    }
  }

  onStepBack() {
    this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/2`]);
  }

  onDataChecking() {
    // perform all check on file
    this.id_mapping = this.mappingListForm.value.id_mapping;
    this.spinner = true;
    this._ds
      .dataChecker(
        this.stepData.importId,
        this.idFieldMapping,
        this.id_mapping
      )
      .subscribe(
        res => {
          this.spinner = false;
          //this.contentMapRes = res;
          this._ds.getErrorList(this.stepData.importId).subscribe(err => {
            this.n_errors = err.errors.filter(error => error.error_level == 'ERROR').length;
            this.n_warnings = err.errors.filter(error => error.error_level == 'WARNING').length;
            if (this.n_errors == 0) {
              this.disableNextStep = false;
              this.showValidateMappingBtn = false;
            }
          })
          this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/4`]);

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

  createOrUpdateMapping(temporary) {
    this._ds.updateContentMapping(this.id_mapping, this.contentTargetForm.value).subscribe(d => {
      let step4Data: Step4Data = {
        importId: this.stepData.importId
      };
      let step3Data: Step3Data = this.stepData;
      step3Data.id_content_mapping = this.id_mapping;
      step3Data.temporaryMapping = temporary;
      this.stepService.setStepData(3, step3Data);
      this.stepService.setStepData(4, step4Data);
      this.onDataChecking();
    })
  }

  // On close modal: ask if save the mapping or not
  saveMappingUpdate(saveBoolean) {
    if (saveBoolean) {
      this.createOrUpdateMapping(true)
    } else {
      // create a temporary mapping
      const mapping_value = {
        'mappingName': 'mapping_temporaire_' + Date.now(),
        'temporary': true
      }
      this._ds.postMappingName(mapping_value, 'CONTENT').subscribe(id_mapping => {
        this.id_mapping = id_mapping;
        this.createOrUpdateMapping(false)
      })
    }
  }

  goToPreview() {
    // if the form has not be changed

    if (this.contentTargetForm.pristine) {
      this.createOrUpdateMapping(false)
    } else {
      // if the form mapping has been changed and the user has right to update it
      if (this.mappingListForm.value.cruved.U) {
        this._modalService.open(this.modalConfirm);
      }
      else {
        // the user don't has right to update the mapping -> temporary mapping
        const mapping_value = {
          'mappingName': 'mapping_temporaire_' + Date.now(),
          'temporary': true
        }
        this._ds.postMappingName(mapping_value, 'CONTENT').subscribe(id_mapping => {
          this.id_mapping = id_mapping;
          this.createOrUpdateMapping(true)
        })
      }
    }

  }
}
