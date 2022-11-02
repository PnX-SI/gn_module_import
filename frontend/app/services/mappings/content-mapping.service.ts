import { Injectable } from "@angular/core";
import { DataService } from "../data.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../module.config";

@Injectable()
export class ContentMappingService {
  public userContentMappings;
  public newMapping: boolean = false;
  public id_mapping;
  public displayMapped: boolean;

  constructor(
    private _ds: DataService,
    private _commonService: CommonService
  ) {
    this.displayMapped = ModuleConfig.DISPLAY_MAPPED_VALUES;
  }

  getMappingNamesList(newContentId?, formControl?) {
    // get list of existing content mapping in the select
    this._ds.getContentMappings().subscribe(
      result => {
        this.userContentMappings = result
        if (newContentId) {
          const newMapping = result.find(el => {
            return el.id == newContentId
          })
          formControl.setValue(newMapping)
        }

      }
    );
  }

  createMapping(mappingForm) {
    mappingForm.reset();
    this.newMapping = true;
    this.displayMapped = true;
  }

  cancelMapping(mappingForm) {
    this.newMapping = false;
    mappingForm.controls["mappingName"].setValue("");
  }

}
